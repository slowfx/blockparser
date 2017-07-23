
// Full SQL dump of the blockchain for blockstor

#include <util.h>
#include <stdio.h>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <callback.h>

static uint8_t empty[kSHA256ByteSize] = { 0x42 };

typedef GoogMap<
    Hash256,
    uint64_t,
    Hash256Hasher,
    Hash256Equal
>::Map OutputMap;

struct BlockStor : public Callback {

    FILE *txFile;
    FILE *txoutFile;
    FILE *txinFile;
    FILE *addressFile;
    FILE *statesFile;

    uint32_t txID;
    uint32_t blkID;
    uint32_t blkDateTime;
    uint32_t addrID;
    uint32_t txoutID;
    uint32_t txinID;

    int64_t cutoffBlock;
    OutputMap outputMap;
    OutputMap pubMap;
    optparse::OptionParser parser;

    BlockStor() {
        parser
            .usage("[options] [list of addresses to restrict output to]")
            .version("")
            .description("create an SQL dump of the blockchain")
            .epilog("")
        ;
        parser
            .add_option("-a", "--atBlock")
            .action("store")
            .type("int")
            .set_default(-1)
            .help("stop dump at block <block> (default: all)")
        ;
    }

    virtual const char                   *name() const         { return "blockstor"; }
    virtual const optparse::OptionParser *optionParser() const { return &parser;   }
    virtual bool                       needUpstream() const    { return true;      }

    virtual void aliases(
        std::vector<const char*> &v
    ) const {
        v.push_back("stor");
    }

    virtual int init(
        int argc,
        const char *argv[]
    ) {
        txID = 0;
        blkID = 0;
        addrID = 0;
        txoutID = 0;
        txinID = 0;

        static uint64_t sz = 32 * 1000 * 1000;
        outputMap.setEmptyKey(empty);
        outputMap.resize(sz);
        pubMap.setEmptyKey(empty);
        pubMap.resize(sz);

        optparse::Values &values = parser.parse_args(argc, argv);
        cutoffBlock = values.get("atBlock").asInt64();
        if(cutoffBlock >= 0) {
            info("cutoff block = %ld\n", cutoffBlock);
        }

        info("dumping the blockchain ...");

        txFile = fopen("txs.txt", "w");
        if(!txFile) sysErrFatal("couldn't open file txs.txt for writing\n");

        txoutFile = fopen("txouts.txt", "w");
        if(!txoutFile) sysErrFatal("couldn't open file txouts.txt for writing\n");

        txinFile = fopen("txins.txt", "w");
        if(!txinFile) sysErrFatal("couldn't open file txins.txt for writing\n");

        addressFile = fopen("addresses.txt", "w");
        if(!addressFile) sysErrFatal("couldn't open file addresses.txt for writing\n");

        statesFile = fopen("states.txt", "w");
        if(!statesFile) sysErrFatal("couldn't open file states.txt for writing\n");

        FILE *sqlFile = fopen("blockstor.sql", "w");
        if(!sqlFile) sysErrFatal("couldn't open file blockstor.sql for writing\n");

        fprintf(
            sqlFile,
            "\n"
            "DROP DATABASE IF EXISTS blockstor;\n"
            "CREATE DATABASE blockstor;\n"
            "USE blockstor;\n"
            "\n"
            "DROP TABLE IF EXISTS txs;\n"
            "DROP TABLE IF EXISTS txouts;\n"
            "DROP TABLE IF EXISTS txins;\n"
            "DROP TABLE IF EXISTS addresses;\n"
            "DROP TABLE IF EXISTS states;\n"
            "DROP TABLE IF EXISTS voidtxs;\n"
            "\n"
            "CREATE TABLE txs(\n"
            "    id int(11) unsigned NOT NULL auto_increment primary key,\n"
            "    tx BINARY(32) NOT NULL,\n"
            "    height int(11) unsigned NOT NULL,\n"
            "    time datetime NOT NULL\n"
            // unique(tx)
            // index(height)
            ");\n"
            "\n"
            "CREATE TABLE txouts(\n"
            "    id int(11) unsigned NOT NULL auto_increment primary key,\n"
            "    tx_id int(11) unsigned NOT NULL,\n"
            "    n int(11) unsigned NOT NULL,\n"
            "    value bigint NOT NULL,\n"
            "    address_id int(11) unsigned,\n"
            "    spent tinyint(1) unsigned\n" // spent will be updated later
            // index(tx_id, n)
            // index(address_id)
            ");\n"
            "\n"
            "CREATE TABLE txins(\n"
            "    id int(11) unsigned NOT NULL auto_increment primary key,\n"
            "    tx_id int(11) unsigned NOT NULL,\n"
            "    n int(11) unsigned NOT NULL,\n"
            "    ref_id int(11) unsigned NOT NULL\n"
            // index(tx_id)
            // index(ref_id)
            ");\n"
            "\n"
            "CREATE TABLE addresses(\n"
            "    id int(11) unsigned NOT NULL auto_increment primary key,\n"
            "    address varchar(96)\n"
            // unique(address)
            ");\n"
            "\n"
            "CREATE TABLE states(\n"
            "    id int(11) unsigned NOT NULL primary key,\n"
            "    last_height int(11) unsigned NOT NULL\n"
            ");\n"
            "\n"
        );

        fclose(sqlFile);

        FILE *bashFile = fopen("blockstor.bash", "w");
        if(!bashFile) sysErrFatal("couldn't open file blockstor.bash for writing\n");

        fprintf(
            bashFile,
            "\n"
            "#!/bin/bash\n"
            "\n"
            "MYSQL_DB_NAME='blockstor'\n"
            "MYSQL_USER='root'\n"
            "MYSQL_PASSWORD=''\n"
            "MYSQL_HOST='localhost'\n"
            "\n"
            "echo\n"
            "\n"
            "echo 'wiping/re-creating DB blockstor ...'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" < blockstor.sql\n"
            "echo done\n"
            "echo\n"
            "\n"
            "for i in txs txouts txins addresses states\n"
            "do\n"
            "    echo Importing table $i ...\n"
            "    time mysqlimport -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" --lock-tables --use-threads=3 --local $MYSQL_DB_NAME $i.txt\n"
            "    echo done\n"
            "    echo\n"
            "done\n"
            "echo 'add index txs unique(tx)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txs add unique(tx);\"\n"
            "echo done\n"
            "echo\n"
            "echo 'add index txs index(height)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txs add index(height);\"\n"
            "echo done\n"
            "echo\n"
            "echo 'add index txouts index(tx_id, n)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txouts add index(tx_id, n);\"\n"
            "echo done\n"
            "echo\n"
            "echo 'add index txouts index(address_id)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txouts add index(address_id);\"\n"
            "echo done\n"
            "echo\n"

            // Depends on your need
            //"echo 'add index txins index(tx_id)'\n"
            //"time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txins add index(tx_id);\"\n"
            //"echo done\n"
            //"echo\n"
            "echo 'add index txins index(ref_id)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table txins add index(ref_id);\"\n"
            "echo done\n"
            "echo\n"

            "echo 'add index addresses unique(address)'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; alter table addresses add unique(address);\"\n"
            "echo done\n"
            "echo\n"
            "echo 'update txouts spent from txins'\n"
            "time mysql -u $MYSQL_USER -p -h$MYSQL_HOST --password=\"$MYSQL_PASSWORD\" -e \"use $MYSQL_DB_NAME; update txouts inner join txins on txouts.tx_id = txins.tx_id and txouts.n = txins.n set txouts.spent = 1;\"\n"
            "echo done\n"
            "echo\n"
            "\n"

            // show warnings: mysql -u root -p -e "use blockstor; LOAD DATA LOCAL INFILE 'txouts.txt' INTO TABLE txouts; SHOW WARNINGS"
        );
        fclose(bashFile);

        return 0;
    }

    virtual void startBlock(
        const Block *b,
        uint64_t
    ) {
        if(0 <= cutoffBlock && cutoffBlock < b->height) {
            wrapup();
        }

        auto p = b->chunk->getData();
        uint8_t blockHash[kSHA256ByteSize];

        #if defined(DARKCOIN)
            h9(blockHash, p, 80);
            SKIP(uint32_t, version, p);
        #elif defined(PAYCON)
            h13(blockHash, p, 80);
            SKIP(uint32_t, version, p);
        #elif defined(CLAM)
            auto pBis = p;
            LOAD(uint32_t, version, pBis);
            if(6<version) {
                sha256Twice(blockHash, p, 80);
            } else {
                scrypt(blockHash, p, 80);
            }
        #elif defined(JUMBUCKS)
            scrypt(blockHash, p, 80);
            SKIP(uint32_t, version, p);
        #elif defined(BITZENY)
            yescrypt(blockHash, p, 80);
            SKIP(uint32_t, version, p);
        #else
            sha256Twice(blockHash, p, 80);
            SKIP(uint32_t, version, p);
        #endif

        SKIP(uint256_t, prevBlkHash, p);
        SKIP(uint256_t, blkMerkleRoot, p);
        LOAD(uint32_t, blkTime, p);

        blkID = (uint32_t)b->height-1;

        blkDateTime = blkTime;

        if(0==(b->height)%500) {
            fprintf(
                stderr,
                "block=%8" PRIu64 " "
                "nbOutputs=%8" PRIu64 "\n",
                b->height,
                outputMap.size()
            );
        }
    }

    virtual void startTX(
        const uint8_t *p,
        const uint8_t *hash
    ) {
        const time_t tt = (time_t)blkDateTime;
        auto timeinfo = gmtime(&tt);
        char timeBuf[20];

        strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S", timeinfo);

        fprintf(txFile, "%" PRIu32 "\t", ++txID);

        writeEscapedBinaryBufferRev(txFile, hash, kSHA256ByteSize);
        fputc('\t', txFile);

        fprintf(txFile, "%" PRIu32 "\t"
                        "%s\n", blkID, timeBuf);
    }

    virtual void endOutput(
        const uint8_t *p,
        uint64_t      value,
        const uint8_t *txHash,
        uint64_t      outputIndex,
        const uint8_t *outputScript,
        uint64_t      outputScriptSize
    ) {
        uint8_t address[40];
        address[0] = 'X';
        address[1] = 0;

        uint8_t addrType[3];
        uint160_t pubKeyHash;
        int type = solveOutputScript(
            pubKeyHash.v,
            outputScript,
            outputScriptSize,
            addrType
        );

        if(likely(0<=type)) {
            uint32_t addr_id;
            auto src = pubMap.find(pubKeyHash.v);
            if(pubMap.end()==src) {
                hash160ToAddr(
                    address,
                    pubKeyHash.v,
                    false,
                    addrType[0]
                );

                uint8_t *h = allocHash256();
                memcpy(h, pubKeyHash.v, kSHA256ByteSize);
                addr_id = ++addrID;
                pubMap[h] = addr_id;

                fprintf(
                    addressFile,
                    "%" PRIu32 "\t"
                    "%s\n"
                    ,
                    addr_id,
                    address
                );
            } else {
                addr_id = src->second;
            }

            fprintf(
                txoutFile,
                "%" PRIu32 "\t"
                "%" PRIu32 "\t"
                "%" PRIu32 "\t"
                "%" PRIu64 "\t"
                "%" PRIu32 "\t"
                "%" PRIu32 "\n",
                ++txoutID,
                txID,
                (uint32_t)outputIndex,
                value,
                addr_id,
                (uint32_t)0
            );
        } else {
            fprintf(
                txoutFile,
                "%" PRIu32 "\t"
                "%" PRIu32 "\t"
                "%" PRIu32 "\t"
                "%" PRIu64 "\t"
                "\\N\t"
                "%" PRIu32 "\n",
                ++txoutID,
                txID,
                (uint32_t)outputIndex,
                value,
                (uint32_t)0
            );
        }

        uint32_t oi = outputIndex;
        uint8_t *h = allocHash256();
        memcpy(h, txHash, kSHA256ByteSize);

        uintptr_t ih = reinterpret_cast<uintptr_t>(h);
        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
        h32[0] ^= oi;

        outputMap[h] = txID;
    }

    virtual void edge(
        uint64_t      value,
        const uint8_t *upTXHash,
        uint64_t      outputIndex,
        const uint8_t *outputScript,
        uint64_t      outputScriptSize,
        const uint8_t *downTXHash,
        uint64_t      inputIndex,
        const uint8_t *inputScript,
        uint64_t      inputScriptSize
    ) {
        uint256_t h;
        uint32_t oi = outputIndex;
        memcpy(h.v, upTXHash, kSHA256ByteSize);

        uintptr_t ih = reinterpret_cast<uintptr_t>(h.v);
        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
        h32[0] ^= oi;

        auto src = outputMap.find(h.v);
        if(outputMap.end()==src) {
            errFatal("unconnected input");
        }

        fprintf(
            txinFile,
            "%" PRIu32 "\t"
            "%" PRIu32 "\t"
            "%" PRIu32 "\t"
            "%" PRIu32 "\n",
            ++txinID,
            (uint32_t)src->second,
            (uint32_t)outputIndex,
            txID
        );
    }

    virtual void wrapup() {
        fprintf(statesFile, "1\t%" PRIu32 "\n", blkID);

        fclose(statesFile);
        fclose(addressFile);
        fclose(txinFile);
        fclose(txoutFile);
        fclose(txFile);
        info("done\n");
    }
};

static BlockStor blockStor;
