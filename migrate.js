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

// 🛑 IMPORTANT: These are the collection names
const OLD_VISITS_COLLECTION = 'visits';
const NEW_SHIPS_COLLECTION = 'ships';
const NEW_VISITS_COLLECTION = 'visits_new';
const NEW_TRIPS_COLLECTION = 'trips';

// Map to store shipName/IMO -> new Ship ID for de-duplication
const shipMap = new Map();

// --- 2. CONFIGURATION: PILOT REASSIGNMENT CUTOFF DATES (Assuming 2025 as a contextually relevant year) ---

// Define cutoffs as milliseconds for easy comparison against trip boarding time
const FERGAL_WILL_CUTOFF_MS = new Date('2025-06-25T00:00:00Z').getTime();
const FINTAN_MATT_CUTOFF_MS = new Date('2025-06-23T00:00:00Z').getTime();


/**
 * --- 3. ROBUST DATA CONVERSION HELPERS ---
 */

/**
 * Safely converts ANY date-like value into a valid Firestore Timestamp.
 */
function safeToTimestamp(dateField, fallbackToNow = true) {
    if (!dateField) {
        return fallbackToNow ? Timestamp.now() : null;
    }
    if (dateField instanceof Timestamp) {
        return dateField;
    }

    let dateObject = null;
    if (dateField instanceof Date) {
        dateObject = dateField;
    }
    if (typeof dateField === 'number' || (typeof dateField === 'string' && dateField.length > 0)) {
        try {
            dateObject = new Date(dateField);
        } catch (e) {
            return fallbackToNow ? Timestamp.now() : null;
        }
    }
    if (dateField && typeof dateField === 'object' && dateField.seconds && typeof dateField.seconds === 'number') {
        return new Timestamp(dateField.seconds, dateField.nanoseconds || 0);
    }
    if (dateObject && !isNaN(dateObject.getTime())) {
        return Timestamp.fromDate(dateObject);
    }
    return fallbackToNow ? Timestamp.now() : null;
}

/**
 * Extracts a 7-digit IMO number from a Marine Traffic URL string.
 */
function extractImoFromUrl(url) {
    if (!url || typeof url !== 'string') return null;
    const match = url.match(/imo:(\d{7})/i);
    if (match && match[1]) {
        return match[1];
    }
    return null;
}


/**
 * --- 4. TRIP DATA TRANSFORMER ---
 */
function transformTrip(oldTripData, typeTrip, visitId, shipId, recordedBy) {
    // This now uses the (potentially updated) pilot from the main loop
    const boardingTime = safeToTimestamp(oldTripData.boarding);

    const newTrip = {
        visitId: visitId,
        shipId: shipId,
        typeTrip: typeTrip,
        boarding: boardingTime,
        pilot: oldTripData.pilot || 'Unknown Pilot',
        fromPort: oldTripData.fromPort || null,
        toPort: oldTripData.port || null,

        pilotNotes: oldTripData.preTripNote || oldTripData.note || null,
        extraChargesNotes: oldTripData.extra || null,
        isConfirmed: oldTripData.confirmed === true,

        ownNote: oldTripData.ownNote || null,
        pilotNo: oldTripData.pilotNo || null,
        monthNo: oldTripData.monthNo || null,
        car: oldTripData.car || null,
        timeOff: safeToTimestamp(oldTripData.timeOff, false),
        good: oldTripData.good || null,

        recordedBy: recordedBy,
        recordedAt: Timestamp.now(),
    };

    if (oldTripData.typeTrip) {
        newTrip.typeTrip = oldTripData.typeTrip;
    }

    return newTrip;
}


/**
 * --- 5. MAIN MIGRATION LOGIC ---
 */
async function runMigration() {
    console.log('🚀 Starting data migration...');

    try {
        const snapshot = await db.collection(OLD_VISITS_COLLECTION).get();

        if (snapshot.empty) {
            console.log('✅ No documents found in the old visits collection. Migration complete.');
            return;
        }

        console.log(`\nFound ${snapshot.size} old visit documents to process.`);

        let batch = db.batch();
        let batchCount = 0;
        const recordedBy = 'Migration_Script_Pilot';

        for (const doc of snapshot.docs) {
            const oldVisit = doc.data();
            const oldVisitId = doc.id;

            const shipInfo = oldVisit.shipInfo || {
                ship: oldVisit.ship,
                gt: oldVisit.gt,
                imo: oldVisit.imoNumber || oldVisit.imo,
                marineTrafficLink: oldVisit.marineTraffic,
                shipnote: oldVisit.shipNote
            };

            const shipName = (shipInfo.ship || '').trim();

            // --- IMO Number Extraction ---
            let imoNumber = shipInfo.imo || null;

            if (!imoNumber && shipInfo.marineTrafficLink) {
                imoNumber = extractImoFromUrl(shipInfo.marineTrafficLink);
            }
            if (imoNumber) {
                imoNumber = parseInt(imoNumber);
                if (isNaN(imoNumber)) imoNumber = null;
            }

            if (!shipName) {
                console.warn(`⚠️ Skipping old visit ${oldVisitId}: No ship name found.`);
                continue;
            }

            // --- A. Handle Ship De-duplication (Collection: /ships) ---
            let shipId;
            const shipKey = imoNumber ? `imo_${imoNumber}` : `name_${shipName.toLowerCase()}`;

            if (shipMap.has(shipKey)) {
                shipId = shipMap.get(shipKey);
            } else {
                const shipRef = db.collection(NEW_SHIPS_COLLECTION).doc();
                shipId = shipRef.id;
                shipMap.set(shipKey, shipId);

                const newShip = {
                    id: shipId,
                    shipName: shipName,
                    grossTonnage: shipInfo.gt || 0,
                    imoNumber: imoNumber,
                    marineTrafficLink: shipInfo.marineTrafficLink || shipInfo.marineTraffic || null,
                    shipNotes: shipInfo.shipnote || null,
                    createdAt: Timestamp.now(),
                    updatedAt: Timestamp.now(),
                };
                batch.set(shipRef, newShip);
                batchCount++;
            }

            // --- B. Transform and Write Visit (Collection: /visits_new) ---
            const visitRef = db.collection(NEW_VISITS_COLLECTION).doc(oldVisitId);
            const visitStatus = oldVisit.status || 'Due';

            const newVisit = {
                id: oldVisitId,
                shipId: shipId,
                shipName: shipName,
                grossTonnage: shipInfo.gt || 0,
                currentStatus: visitStatus,
                initialEta: safeToTimestamp(oldVisit.eta),
                berthPort: oldVisit.berth || null,
                statusLastUpdated: safeToTimestamp(oldVisit.updateTime),
                updatedBy: oldVisit.updatedBy || oldVisit.updateUser || recordedBy,
                visitNotes: oldVisit.note || null,
            };
            batch.set(visitRef, newVisit);
            batchCount++;


            // --- C. Transform and Write Trips (Collection: /trips) ---

            const tripsToProcess = [];

            // 1. Inward Trip: APPLY PILOT REASSIGNMENT LOGIC HERE
            if (oldVisit.inward && oldVisit.inward.boarding) {
                const inboundPilot = oldVisit.inward.pilot;

                // Safely get boarding time in milliseconds for comparison
                const boardingTimestamp = safeToTimestamp(oldVisit.inward.boarding);
                const boardingTimeMs = boardingTimestamp.toMillis();

                // Logic 1: Fergal -> Will
                if (inboundPilot === 'Fergal' && boardingTimeMs > FERGAL_WILL_CUTOFF_MS) {
                    oldVisit.inward.pilot = 'Will';
                    console.log(`   Pilot reassigned: Fergal -> Will for Visit ${oldVisitId}`);
                }
                // Logic 2: Fintan -> Matt
                else if (inboundPilot === 'Fintan' && boardingTimeMs > FINTAN_MATT_CUTOFF_MS) {
                    oldVisit.inward.pilot = 'Matt';
                    console.log(`   Pilot reassigned: Fintan -> Matt for Visit ${oldVisitId}`);
                }

                // Transform the potentially modified trip data
                const inTrip = transformTrip(oldVisit.inward, 'In', oldVisitId, shipId, recordedBy);
                inTrip.isConfirmed = oldVisit.inwardConfirmed === true || inTrip.isConfirmed;
                tripsToProcess.push(inTrip);
            } else {
                console.warn(`  ⚠️ Missing In Trip for ${shipName} (${oldVisitId})`);
            }

            // 2. Outward Trip (No pilot reassignment needed as per request)
            if (oldVisit.outward && oldVisit.outward.boarding) {
                const outTrip = transformTrip(oldVisit.outward, 'Out', oldVisitId, shipId, recordedBy);
                outTrip.isConfirmed = oldVisit.outwardConfirmed === true || outTrip.isConfirmed;
                tripsToProcess.push(outTrip);
            }

            // 3. Extra Trips
            const extraTrips = oldVisit.extra || oldVisit.trips || [];

            if (Array.isArray(extraTrips) && extraTrips.length > 0) {
                for (const extraTrip of extraTrips) {
                    if (extraTrip && extraTrip.boarding && extraTrip.typeTrip) {
                        // NOTE: Pilot reassignment is only applied to INWARD trips as requested.
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
                batchCount++;
            }


            // --- D. Commit Batch if full ---
            if (batchCount >= 490) {
                console.log(`   ⏳ Committing batch of ${batchCount} operations...`);
                await batch.commit();
                batch = db.batch();
                batchCount = 0;
            }
        }

        // --- 6. Final Commit ---
        if (batchCount > 0) {
            console.log(`\n   ✅ Committing final batch of ${batchCount} operations...`);
            await batch.commit();
        }

        console.log(`\n🎉 MIGRATION SUCCESSFUL!`);
        console.log(`Total Ships created/updated: ${shipMap.size}`);
        console.log(`All ${snapshot.size} visits migrated.`);

    } catch (error) {
        console.error(`\n🛑 CRITICAL ERROR during migration:`, error);
    }
}

runMigration();