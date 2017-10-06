
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.transactions.TransactionState;

/**
 * This example demonstrates how to iterate over archive WAL segments with special handling of Logical records
 * <ul>
 * <li>Data record and its data entries</li>
 * <li>Transactions record</li>
 * </ul>
 */
public class WalReaderExample {

    /** End line constant for output into file */
    private static final String ENDL = String.format("%n");

    /** Type name of class which used for test. */
    private static final String TYPE_NAME = "org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IndexedObject";

    /**
     * Main example entry point
     *
     * @param args ignored, but can be modified to take WAL fork directory from input parameters
     * @throws Exception if failed
     */
    public static void main(String[] args) throws Exception {
        //Archive WAL segments folder including consistent ID
        final File walFilesFolder = new File("./persistent_store/db/wal/archive/127_0_0_1_47500");

        //Binary metadata folder including consistent ID
        final File binaryMeta = new File("./persistent_store/binary_meta/127_0_0_1_47500");

        //Marshaller cache folder
        final File marshaller = new File("./persistent_store/marshaller");

        // Text file for output results
        final File outputDumpFile = new File("wal.dump.txt");

        //page size of Ignite Persistent Data store to read WAL from
        final int pageSize = 4096;

        final JavaLogger log = new JavaLogger();

        //this flags disables unmarshalling (unwrapping) binary objects into original object
        final boolean keepBinary = true;

        final IgniteWalIteratorFactory factory
            = new IgniteWalIteratorFactory(log,
            pageSize,
            binaryMeta,
            marshaller,
            keepBinary);

        final File[] walFileList = walFilesFolder.listFiles((dir, name) -> name.endsWith(".wal"));
        A.ensure(walFileList != null, "Can't find any segments in [" + walFilesFolder + "]");
        int cnt = 0;
        final Map<GridCacheVersion, Integer> uniqueTxFound = new HashMap<>();
        int cntEntries = 0;

        try (WALIterator iter = factory.iteratorArchiveFiles(walFileList)) {
            try (FileWriter writer = new FileWriter(outputDumpFile)) {
                while (iter.hasNextX()) {
                    final IgniteBiTuple<WALPointer, WALRecord> next = iter.nextX();
                    final WALRecord walRecord = next.get2();

                    if (walRecord.type() == WALRecord.RecordType.DATA_RECORD && walRecord instanceof DataRecord) {
                        final DataRecord dataRecord = (DataRecord)walRecord;
                        handleDataRecord(writer, dataRecord, uniqueTxFound);
                        cntEntries += dataRecord.writeEntries().size();
                    }
                    else if (walRecord.type() == WALRecord.RecordType.TX_RECORD && walRecord instanceof TxRecord) {
                        final TxRecord txRecord = (TxRecord)walRecord;
                        handleTxRecord(writer, txRecord);
                    }
                    writer.write(walRecord.toString() + ENDL);

                    cnt++;
                }
            }
        }
        System.out.println(
            "Data from WAL archive [" + walFilesFolder.getAbsolutePath() + "]" +
                " was converted to [" + outputDumpFile.getAbsolutePath() + "]");
        System.out.println(cnt + " WAL records were processed ");
        System.out.println(cntEntries + " entry operations was found under ");
        System.out.println(uniqueTxFound.size() + " transactions");
        uniqueTxFound.entrySet().forEach(e-> System.out.println(" -> Transactional entries "+ e));

    }

    /**
     * Handles logical data record with cache operation description.
     * This record contains information about operation we want to do.
     * Contains operation type (put, remove) and (Key, Value, Version) for each {@link DataEntry}
     *
     * @param writer writer for output record details
     * @param dataRecord record to process
     * @param uniqueTxFound Map for collecting unique transactions observed in log, maps nearXidVersion to entries
     * count
     * @throws IOException if debug output building failed
     */
    private static void handleDataRecord(Writer writer,
        DataRecord dataRecord,
        Map<GridCacheVersion, Integer> uniqueTxFound) throws IOException {
        final List<DataEntry> entries = dataRecord.writeEntries();

        for (DataEntry entry : entries) {
            final GridCacheVersion globalTxId = entry.nearXidVersion();

            writer.write("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                "under transaction: " + globalTxId + "; "  + ENDL);

            //after successful unmarshalling all entries should be already unwrapped
            if (entry instanceof UnwrapDataEntry) {
                final UnwrapDataEntry lazyDataEntry = (UnwrapDataEntry)entry;
                Object key = lazyDataEntry.unwrappedKey();
                Object val = lazyDataEntry.unwrappedValue();
                System.out.println(lazyDataEntry.op() + " found for entry (" + key + "->" + val + ")");
                handleObject(key);
                handleObject(val);
            }
            if (globalTxId != null)
                uniqueTxFound.merge(globalTxId, 1, (i, j) -> i + j);
        }
    }

    /**
     * Prints fields of binary objects
     *
     * See also documentation section for more details: https://apacheignite.readme.io/docs/binary-marshaller
     *
     * @param v value to be processed
     */
    private static void handleObject(Object v) {
        if (v instanceof BinaryObject) {
            BinaryObject binaryObj = (BinaryObject)v;

            // Following section demonstrate how to handle well-known binary object fields {{
            if (TYPE_NAME.equals(binaryObj.type().typeName())) {
                int iValField = binaryObj.field("iVal");
                int jValField = binaryObj.field("jVal");
                //original test was producing only same values of iVal & jVal
                assert iValField == jValField :
                    "\tFields found in indexed object: i=" + iValField + ", j=" + jValField + " are not the same";
            }
            // }} - such code may be written only if it is well known that object has these fields

            //this section demonstrates how to handle binary object fields;
            final BinaryType type = binaryObj.type();

            //see also https://apacheignite.readme.io/docs/binary-marshaller#binaryobject-cache-api for more info

            System.out.print("\tBinary object fields: ");
            for (String fieldName : type.fieldNames()) {
                Object field = binaryObj.field(fieldName);
                System.out.print(fieldName +" (" + type.fieldTypeName(fieldName) + ") = " + field + ", ");
            }
            System.out.println();
        }
        else
            System.out.println("\tGeneric value: ("+v.getClass().getSimpleName()+") = " + v);
    }

    /**
     * Handles logical data record indented for transaction (tx) related actions.<br>
     * This record is marker of begin, prepare, commit, and rollback transactions.
     *
     * @param writer output for action comments
     * @param txRecord record to process
     * @throws IOException if failed to prepare file output
     */
    private static void handleTxRecord(Writer writer, TxRecord txRecord) throws IOException {
        final GridCacheVersion globalTxId = txRecord.nearXidVersion();

        final TransactionState act = txRecord.state();
        switch (act) {
            case PREPARING:
                //here special handling may be inserted for transaction prepare action
                break;
            case COMMITTING:
                //here special handling may be inserted for transaction commit action
                break;
        }
        writer.write("//Tx Record, action: " + act +
            "; nearTxVersion" + globalTxId + ENDL);
    }
}
