// --- 1. SETUP AND INITIALIZATION ---

const admin = require('firebase-admin');

// 🛑 CRITICAL: Path to your downloaded service account key.
const serviceAccount = require('./serviceAccountKey.json');

// Initialize the Firebase Admin SDK
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const Timestamp = admin.firestore.Timestamp;

// 🛑 IMPORTANT: Change this if your old collection is named differently
const OLD_VISITS_COLLECTION = 'visits';
// 🛑 IMPORTANT: These are the NEW normalized collections (as per your data.model.ts)
const NEW_SHIPS_COLLECTION = 'ships';
const NEW_VISITS_COLLECTION = 'visits_new'; // Renamed to avoid collision with old collection for safety
const NEW_TRIPS_COLLECTION = 'trips';
const NEW_METADATA_COLLECTION = 'system_metadata'; // For storing aggregate counts

// Map to store shipName/IMO -> new Ship ID for de-duplication
const shipMap = new Map();


/**
 * --- 2. HELPER FUNCTIONS ---
 * * Transforms an old nested trip object into a new standalone Trip document.
 * * @param {object} oldTripData - The 'inward', 'outward', or 'extra' trip object.
 * @param {string} typeTrip - The explicit type ('In', 'Out', 'Shift', etc.)
 * @param {string} visitId - The ID of the parent Visit document.
 * @param {string} shipId - The ID of the parent Ship document.
 * @param {string} recordedBy - The user who recorded the trip (e.g., "Migration Script").
 * @returns {object} The new Trip document structure.
 */
function transformTrip(oldTripData, typeTrip, visitId, shipId, recordedBy) {
    // 🛑 CRITICISM: Old model uses 'Date' for boarding, new model uses 'Timestamp'.
    // We must convert it or use the Firestore Timestamp utility.
    let boardingTime = oldTripData.boarding;
    if (!(boardingTime instanceof Timestamp) && boardingTime instanceof Date) {
        boardingTime = Timestamp.fromDate(boardingTime);
    } else if (typeof boardingTime === 'string' || typeof boardingTime === 'number') {
        // Assume it's a date string or millisecond number if not a Firestore/JS Date
        boardingTime = Timestamp.fromDate(new Date(oldTripData.boarding));
    }

    // Defaulting to null/empty string as per new model definition
    // (dolanennis/triprecord/triprecord-d05432e9387f17e6c696d87ae0583d8ed8f98d7f/src/app/models/data.model.ts)
    const newTrip = {
        visitId: visitId,
        shipId: shipId,
        typeTrip: typeTrip,
        boarding: boardingTime || Timestamp.now(), // Fallback for safety
        pilot: oldTripData.pilot || 'Unknown Pilot',
        fromPort: oldTripData.fromPort || null,
        // NOTE: Old trip model used 'port' as the destination. We map this to 'toPort'.
        toPort: oldTripData.port || null,

        // Notes & Billing Mapping
        pilotNotes: oldTripData.preTripNote || oldTripData.note || null, // Maps from old preTripNote/note
        extraChargesNotes: oldTripData.extra || null, // Maps from old 'extra' field

        // Confirmed state
        // NOTE: For 'extra' trips, we use the embedded 'confirmed' field.
        // For In/Out, we rely on the parent Visit property which is handled in the main function.
        isConfirmed: oldTripData.confirmed === true,

        // Pilot Optional Fields (from spa/src/app/shared/trip.model.ts)
        ownNote: oldTripData.ownNote || null,
        pilotNo: oldTripData.pilotNo || null,
        monthNo: oldTripData.monthNo || null,
        car: oldTripData.car || null,
        timeOff: oldTripData.timeOff || null,
        good: oldTripData.good || null,

        // Audit Fields (will be set by the server on a Cloud Function in a proper app, but for migration we set them)
        recordedBy: recordedBy,
        recordedAt: Timestamp.now(),
    };

    // Ensure that if it was an 'extra' trip, the type is correctly named if available
    if (oldTripData.typeTrip) {
        newTrip.typeTrip = oldTripData.typeTrip;
    }

    return newTrip;
}

/**
 * Extracts an IMO number from a MarineTraffic URL.
 * @param {string} url The URL to parse.
 * @returns {string|null} The extracted IMO number as a string, or null if not found.
 */
function extractImoFromUrl(url) {
    if (typeof url !== 'string' || !url) {
        return null;
    }
    // Example URL: https://www.marinetraffic.com/en/ais/details/ships/imo:9100126/
    const imoRegex = /imo:(\d+)/i; // Use 'i' for case-insensitivity
    const match = url.match(imoRegex);
    return match && match[1] ? match[1] : null;
}


/**
 * --- 3. MAIN MIGRATION LOGIC ---
 */
async function runMigration() {
    console.log('🚀 Starting data migration...');

    // Aggregators for final statistics
    const visitCountsByYear = {};
    const tripCountsByYear = {};

    try {
        // 1. Fetch all documents from the old collection
        const snapshot = await db.collection(OLD_VISITS_COLLECTION).get();

        if (snapshot.empty) {
            console.log('✅ No documents found in the old visits collection. Migration complete.');
            return;
        }

        console.log(`\nFound ${snapshot.size} old visit documents to process.`);

        // Use a batch for transactional writes (max 500 operations per batch)
        let batch = db.batch();
        let batchCount = 0;
        const recordedBy = 'Migration_Script_Pilot'; // Audit user

        for (const doc of snapshot.docs) {
            const oldVisit = doc.data();
            const oldVisitId = doc.id;

            // Get Ship info from the old document. The structure is inconsistent:
            // either direct fields (spa) or nested under shipInfo (old triprecord model in trip.model.ts)
            const shipInfo = oldVisit.shipInfo || {
                ship: oldVisit.ship,
                gt: oldVisit.gt,
                imo: oldVisit.imoNumber || oldVisit.imo,
                marineTrafficLink: oldVisit.marineTraffic,
                shipnote: oldVisit.shipNote
            };

            const shipName = (shipInfo.ship || '').trim();
            let imoNumber = shipInfo.imo || null;

            // Attempt to extract IMO from the marineTrafficLink if it's not already present.
            if (!imoNumber) {
                imoNumber = extractImoFromUrl(shipInfo.marineTrafficLink);
            }
            // Per your request, we also need gross tonnage for the de-duplication key.
            const grossTonnage = shipInfo.gt || 0;

            if (!shipName) {
                console.warn(`⚠️ Skipping old visit ${oldVisitId}: No ship name found.`);
                continue;
            }

            // --- A. Handle Ship De-duplication (Collection: /ships) ---
            let shipId;
            let shipRef;
            
            // Prioritize IMO number for de-duplication as it's the most reliable unique key.
            // Fall back to a composite key of name and gross tonnage if IMO is unavailable.
            const shipKey = imoNumber ? `imo_${imoNumber}`
                                      : `name_${shipName.toLowerCase()}_gt_${grossTonnage}`;

            if (shipMap.has(shipKey)) {
                // Ship already created, reuse ID
                shipId = shipMap.get(shipKey);
            } else {
                // Create a new Ship document
                shipRef = db.collection(NEW_SHIPS_COLLECTION).doc();
                shipId = shipRef.id;
                shipMap.set(shipKey, shipId);

                const newShip = {
                    id: shipId,
                    shipName: shipName,
                    grossTonnage: grossTonnage,
                    imoNumber: imoNumber,
                    marineTrafficLink: shipInfo.marineTrafficLink || shipInfo.marineTraffic || null,
                    shipNotes: shipInfo.shipnote || null,
                    createdAt: Timestamp.now(), // Since original creation date is not in old model
                    updatedAt: Timestamp.now(),
                };
                batch.set(shipRef, newShip);
                batchCount++;
            }

            // --- B. Transform and Write Visit (Collection: /visits) ---

            // NOTE: We are intentionally setting the new visit ID to the OLD visit ID for
            // simple cross-referencing and auditing if needed. We use a different collection
            // name (`visits_new`) in the script to avoid direct overwrite of the old data.
            const visitRef = db.collection(NEW_VISITS_COLLECTION).doc(oldVisitId);

            // Map old status to new VisitStatus ('Due' | 'Awaiting Berth' | 'Alongside' | 'Sailed' | 'Cancelled')
            const visitStatus = oldVisit.status || 'Due';

            const newVisit = {
                id: oldVisitId,
                shipId: shipId,

                // Denormalized fields from Ship
                shipName: shipName,
                grossTonnage: grossTonnage,

                // Visit Status
                currentStatus: visitStatus,
                // The old 'eta' could be Date or Timestamp, we handle conversion in transformTrip function
                initialEta: oldVisit.eta ? Timestamp.fromDate(new Date(oldVisit.eta)) : Timestamp.now(),
                berthPort: oldVisit.berth || null, // from spa/src/app/shared/visit.model.ts

                // State Change & Audit (using existing or default values)
                statusLastUpdated: oldVisit.updateTime ? Timestamp.fromDate(new Date(oldVisit.updateTime)) : Timestamp.now(),
                updatedBy: oldVisit.updatedBy || oldVisit.updateUser || recordedBy,
                visitNotes: oldVisit.note || null,
            };
            batch.set(visitRef, newVisit);
            batchCount++;

            // --- AGGREGATE VISIT STATS ---
            if (newVisit.initialEta) {
                const visitYear = newVisit.initialEta.toDate().getFullYear();
                visitCountsByYear[visitYear] = (visitCountsByYear[visitYear] || 0) + 1;
            }


            // --- C. Transform and Write Trips (Collection: /trips) ---

            const tripsToProcess = [];

            // 1. Inward Trip (required)
            if (oldVisit.inward && oldVisit.inward.boarding) {
                const inTrip = transformTrip(oldVisit.inward, 'In', oldVisitId, shipId, recordedBy);
                // Use the visit-level confirmation if the trip-level one is missing
                inTrip.isConfirmed = oldVisit.inwardConfirmed === true || inTrip.isConfirmed;
                tripsToProcess.push(inTrip);
            } else {
                console.warn(`  ⚠️ Missing In Trip for ${shipName} (${oldVisitId})`);
            }

            // 2. Outward Trip (required)
            if (oldVisit.outward && oldVisit.outward.boarding) {
                const outTrip = transformTrip(oldVisit.outward, 'Out', oldVisitId, shipId, recordedBy);
                outTrip.isConfirmed = oldVisit.outwardConfirmed === true || outTrip.isConfirmed;
                tripsToProcess.push(outTrip);
            } // An outward trip might legitimately be missing if the ship is still "Alongside" or "Due"

            // 3. Extra Trips (Shift/Anchorage/Other)
            // Check both possible old fields: the SPA 'extra' array or the newer triprecord 'trips' array
            const extraTrips = oldVisit.extra || oldVisit.trips || [];

            if (Array.isArray(extraTrips) && extraTrips.length > 0) {
                for (const extraTrip of extraTrips) {
                    // Check if trip data is valid/complete enough to migrate
                    if (extraTrip && extraTrip.boarding && extraTrip.typeTrip) {
                        const newExtraTrip = transformTrip(extraTrip, extraTrip.typeTrip, oldVisitId, shipId, recordedBy);
                        tripsToProcess.push(newExtraTrip);
                    }
                }
            }

            // Add all generated trips to the batch
            for (const tripData of tripsToProcess) {
                const tripRef = db.collection(NEW_TRIPS_COLLECTION).doc();
                tripData.id = tripRef.id;
                batch.set(tripRef, tripData);

                // --- AGGREGATE TRIP STATS ---
                if (tripData.boarding) {
                    const tripYear = tripData.boarding.toDate().getFullYear();
                    tripCountsByYear[tripYear] = (tripCountsByYear[tripYear] || 0) + 1;
                }
                batchCount++;
            }


            // --- D. Commit Batch if full ---
            if (batchCount >= 490) { // Keep buffer under 500
                console.log(`   ⏳ Committing batch of ${batchCount} operations...`);
                await batch.commit();
                batch = db.batch(); // Start a new batch
                batchCount = 0;
            }
        }

        // --- 4. Final Commit ---
        if (batchCount > 0) {
            console.log(`\n   ✅ Committing final batch of ${batchCount} operations...`);
            await batch.commit();
        }

        // --- 5. Write Aggregate Statistics ---
        console.log('\n   📊 Calculating and writing final statistics...');
        const statsBatch = db.batch();

        // Total ships
        const shipStatsRef = db.collection(NEW_METADATA_COLLECTION).doc('ship_summary');
        statsBatch.set(shipStatsRef, {
            totalShips: shipMap.size,
            lastUpdated: Timestamp.now()
        });

        // Yearly visit counts
        const visitStatsRef = db.collection(NEW_METADATA_COLLECTION).doc('visit_summary');
        statsBatch.set(visitStatsRef, {
            countsByYear: visitCountsByYear,
            lastUpdated: Timestamp.now()
        });

        // Yearly trip counts
        const tripStatsRef = db.collection(NEW_METADATA_COLLECTION).doc('trip_summary');
        statsBatch.set(tripStatsRef, { countsByYear: tripCountsByYear, lastUpdated: Timestamp.now() });

        await statsBatch.commit();

        console.log(`\n🎉 MIGRATION SUCCESSFUL!`);
        console.log(`Total Ships created/updated: ${shipMap.size}`);
        console.log(`All ${snapshot.size} visits migrated.`);

    } catch (error) {
        console.error(`\n🛑 CRITICAL ERROR during migration:`, error);
        // You must check your Firebase console to see which documents were written before the error.
    }
}

runMigration();