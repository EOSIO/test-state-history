const WebSocket = require('ws');
const { Serialize } = require('eosjs');
const fetch = require('node-fetch');
const { TextDecoder, TextEncoder } = require('text-encoding');
const abiAbi = require('./node_modules/eosjs/src/abi.abi.json');
const pg = require('pg');
const zlib = require('zlib');
const commander = require('commander');

const abiTypes = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), abiAbi);

const sqlTypes = {
    bool: { name: 'bool', convert: x => x },
    varuint: { name: 'bigint', convert: x => x },
    varint: { name: 'integer', convert: x => x },
    uint8: { name: 'smallint', convert: x => x },
    uint16: { name: 'integer', convert: x => x },
    uint32: { name: 'bigint', convert: x => x },
    uint64: { name: 'decimal', convert: x => x },
    uint128: { name: 'decimal', convert: x => x },
    int8: { name: 'smallint', convert: x => x },
    int16: { name: 'smallint', convert: x => x },
    int32: { name: 'integer', convert: x => x },
    int64: { name: 'bigint', convert: x => x },
    int128: { name: 'decimal', convert: x => x },
    float64: { name: 'float8', convert: x => x },
    float128: { name: 'bytea', convert: x => x },
    name: { name: 'varchar(13)', convert: x => x },
    time_point: { name: 'varchar', convert: x => x },
    time_point_sec: { name: 'varchar', convert: x => x },
    block_timestamp_type: { name: 'varchar', convert: x => x },
    checksum256: { name: 'varchar(64)', convert: x => x },
    bytes: { name: 'bytea', convert: x => '\\x' + Serialize.arrayToHex(x) },
};

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function toJsonNoBin(x) {
    return JSON.stringify(x, (k, v) => {
        if (v instanceof Uint8Array)
            return "...";
        return v;
    }, 4)
}

class Connection {
    constructor({ socketAddress, receivedAbi, receivedBlock }) {
        this.receivedAbi = receivedAbi;
        this.receivedBlock = receivedBlock;

        this.abi = null;
        this.types = null;
        this.tables = new Map;
        this.blocksQueue = [];
        this.inProcessBlocks = false;

        this.ws = new WebSocket(socketAddress, { perMessageDeflate: false });
        this.ws.on('message', data => this.onMessage(data));
    }

    serialize(type, value) {
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder });
        Serialize.getType(this.types, type).serialize(buffer, value);
        return buffer.asUint8Array();
    }

    deserialize(type, array) {
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
        let result = Serialize.getType(this.types, type).deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: true }));
        if (buffer.readPos != array.length)
            throw new Error('oops: ' + type); // todo: remove check
        // {
        //     console.log(result.actions[0].authorization[0].actor);
        //     //console.log('oops: ' + type);
        // }
        return result;
    }

    toJsonUnpackTransaction(x) {
        return JSON.stringify(x, (k, v) => {
            if (k === 'trx' && Array.isArray(v) && v[0] === 'packed_transaction') {
                const pt = v[1];
                let packed_trx = pt.packed_trx;
                if (pt.compression === 0)
                    packed_trx = this.deserialize('transaction', packed_trx);
                else if (pt.compression === 1)
                    packed_trx = this.deserialize('transaction', zlib.unzipSync(packed_trx));
                return { ...pt, packed_trx };
            }
            if (k === 'packed_trx' && v instanceof Uint8Array)
                return this.deserialize('transaction', v);
            if (v instanceof Uint8Array)
                return `(${v.length} bytes)`;
            return v;
        }, 4)
    }

    send(request) {
        this.ws.send(this.serialize('request', request));
    }

    onMessage(data) {
        try {
            if (!this.abi) {
                this.abi = JSON.parse(data);
                this.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), this.abi);
                for (const table of this.abi.tables)
                    this.tables.set(table.name, table.type);
                if (this.receivedAbi)
                    this.receivedAbi();
            } else {
                const [type, response] = this.deserialize('result', data);
                this[type](response);
            }
        } catch (e) {
            console.log(e);
            process.exit(1);
        }
    }

    requestStatus() {
        this.send(['get_status_request_v0', {}]);
    }

    requestBlocks(requestArgs) {
        this.send(['get_blocks_request_v0', {
            start_block_num: 0,
            end_block_num: 0xffffffff,
            max_messages_in_flight: 5,
            have_positions: [],
            irreversible_only: false,
            fetch_block: false,
            fetch_traces: false,
            fetch_deltas: false,
            ...requestArgs
        }]);
    }

    get_status_result_v0(response) {
        console.log(response);
    }

    get_blocks_result_v0(response) {
        this.blocksQueue.push(response);
        this.processBlocks();
    }

    async processBlocks() {
        if (this.inProcessBlocks)
            return;
        this.inProcessBlocks = true;
        while (this.blocksQueue.length) {
            let response = this.blocksQueue.shift();
            this.send(['get_blocks_ack_request_v0', { num_messages: 1 }]);
            let block, traces = [], deltas = [];
            if (response.block && response.block.length)
                block = this.deserialize('signed_block', response.block);
            if (response.traces && response.traces.length)
                traces = this.deserialize('transaction_trace[]', zlib.unzipSync(response.traces));
            if (response.deltas && response.deltas.length)
                deltas = this.deserialize('table_delta[]', zlib.unzipSync(response.deltas));
            await this.receivedBlock(response, block, traces, deltas);
        }
        this.inProcessBlocks = false;
    }

    forEachRow(delta, f) {
        const type = this.tables.get(delta.name);
        for (let row of delta.rows) {
            let data;
            try {
                data = this.deserialize(type, row.data);
            } catch (e) {
                console.error(e);
            }
            if (data)
                f(row.present, data[1]);
        }
    }

    dumpDelta(delta, extra) {
        this.forEachRow(delta, (present, data) => {
            console.log(this.toJsonUnpackTransaction({ ...extra, present, data }));
        });
    }
} // Connection

class Monitor {
    constructor({ socketAddress }) {
        this.accounts = new Map;
        this.tableIds = new Map;

        this.connection = new Connection({
            socketAddress,
            receivedAbi: () => this.connection.requestBlocks({
                fetch_block: false,
                fetch_traces: false,
                fetch_deltas: true,
            }),
            receivedBlock: async (response, block, traces, deltas) => {
                if (!response.this_block)
                    return;
                if (!(response.this_block.block_num % 100))
                    console.log(`block ${numberWithCommas(response.this_block.block_num)}`)
                if (block)
                    console.log(this.connection.toJsonUnpackTransaction(block));
                if (traces.length)
                    console.log(toJsonNoBin(traces));
                for (let [_, delta] of deltas)
                    //if (delta.name === 'resource_limits_config')
                    this.connection.dumpDelta(delta, { name: delta.name, block_num: response.this_block.block_num });
                for (let [_, delta] of deltas)
                    if (this[delta.name])
                        this[delta.name](response.this_block.block_num, delta);
            }
        });
    }

    getAccount(name) {
        const account = this.accounts.get(name);
        if (!account || !account.rawAbi.length)
            throw new Error('no abi for ' + name);
        if (!account.abi)
            account.abi = abiTypes.get("abi_def").deserialize(new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array: account.rawAbi }));
        if (!account.types)
            account.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), account.abi);
        return account;
    }

    deserializeTable(name, tableName, array) {
        const account = this.getAccount(name);
        const typeName = account.abi.tables.find(t => t.name == tableName).type;
        const type = Serialize.getType(account.types, typeName);
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
        return type.deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: false }));
    }

    account(blockNum, delta) {
        this.connection.forEachRow(delta, (present, data) => {
            if (present && data.abi.length) {
                console.log(`block: ${blockNum} ${data.name}: set abi`);
                this.accounts.set(data.name, { rawAbi: data.abi });
            } else if (this.accounts.has(data.name)) {
                console.log(`block: ${blockNum} ${data.name}: clear abi`);
                this.accounts.delete(data.name);
            }
        });
    }

    contract_row(blockNum, delta) {
        // this.connection.forEachRow(delta, (present, data) => {
        //     if (data.code !== 'eosio.token' && data.table !== 'accounts' || data.scope !== 'eosio')
        //         return;
        //     let content = this.deserializeTable(data.code, data.table, data.value);
        //     console.log(`block: ${blockNum} present: ${present} code:${data.code} scope:${data.scope} table:${data.table} table_payer:${data.payer} payer:${data.payer} primary_key:${data.primary_key}  ${JSON.stringify(content)}`);
        // });
    }

    generated_transaction(blockNum, delta) {
        this.connection.forEachRow(delta, (present, data) => {
            if (data.sender === '.............')
                return;
            console.log('generated_transaction')
            console.log(this.connection.toJsonUnpackTransaction({ present, ...data }));
        });
    }
} // Monitor

class FillPostgress {
    constructor({ socketAddress, schema = 'chain', deleteSchema = false, createSchema = false, irreversibleOnly = false }) {
        this.schema = schema;
        this.pool = new pg.Pool;
        this.sqlTables = new Map;
        this.numRows = 0;

        this.connection = new Connection({
            socketAddress,
            receivedAbi: async () => {
                this.processAbi();
                await this.initDatabase(deleteSchema, createSchema);
                await this.start(irreversibleOnly);
            },
            receivedBlock: this.receivedBlock.bind(this),
        });
    }

    processAbi() {
        for (let abiTable of this.connection.abi.tables) {
            const abiType = Serialize.getType(this.connection.types, abiTable.type).fields[0].type;
            const sqlTable = { name: abiTable.name, abiType, fields: [], insert: '' };
            this.sqlTables.set(sqlTable.name, sqlTable);

            for (let field of abiType.fields) {
                if (!field.type.arrayOf && !field.type.optionalOf && !field.type.fields.length) {
                    let sqlType = sqlTypes[field.type.name];
                    if (!sqlType)
                        throw new Error('unknown type for sql conversion: ' + field.type.name);
                    sqlTable.fields.push({ name: field.name, type: sqlType });
                }
            }
            sqlTable.fields.splice(0, 0,
                { name: 'block_index', type: { name: 'bigint', convert: x => x } },
                { name: 'present', type: { name: 'boolean', convert: x => x } });
            let fieldNames = sqlTable.fields.map(({ name }) => `"${name}"`).join(', ');
            let values = [...Array(sqlTable.fields.length).keys()].map(n => `$${n + 1}`).join(',');
            sqlTable.insert = `insert into ${this.schema}.${sqlTable.name}(${fieldNames}) values (${values})`;
        }
    }

    async initDatabase(deleteSchema = false, createSchema = false) {
        try {
            if (deleteSchema) {
                try {
                    await this.pool.query(`drop schema ${this.schema} cascade`);
                } catch (e) {
                }
            }
            if (createSchema) {
                let client = await this.pool.connect();
                await client.query('start transaction;');
                await client.query(`create schema ${this.schema}`);
                await client.query(`create table ${this.schema}.received_blocks ("block_index" bigint, "block_id" varchar(64), primary key("block_index"));`);
                await client.query(`create table ${this.schema}.status ("head" bigint, "irreversible" bigint);`);
                await client.query(`create unique index on ${this.schema}.status ((true));`);
                await client.query(`insert into ${this.schema}.status values (0, 0);`);
                for (let abiTable of this.connection.abi.tables) {
                    let sqlTable = this.sqlTables.get(abiTable.name);
                    let pk = '"block_index"' + abiTable.key_names.map(x => ',"' + x + '"').join('');
                    // !!! pk: add later?
                    let query = `create table ${this.schema}.${sqlTable.name} (${sqlTable.fields.map(({ name, type }) => `"${name}" ${type.name}`).join(', ')}, primary key(${pk}));`;
                    await client.query(query);
                }
                await client.query('commit;');
                client.release();
            }
        } catch (e) {
            console.log(e);
            process.exit(1);
        }
    }

    async start(irreversible_only) {
        try {
            let status = (await this.pool.query(`select * from ${this.schema}.status`)).rows[0];
            this.head = +status.head;
            this.irreversible = +status.irreversible;

            let have_positions = (await this.pool.query(
                `select * from ${this.schema}.received_blocks where block_index >= ${this.irreversible + 1} and block_index <= ${this.head}`))
                .rows.map(({ block_index, block_id }) => ({ block_num: block_index, block_id }));

            this.connection.requestBlocks({
                irreversible_only,
                start_block_num: this.head + 1,
                have_positions,
                fetch_block: false,
                fetch_traces: false,
                fetch_deltas: true,
            });
        } catch (e) {
            console.log(e);
            process.exit(1);
        }
    }

    async receivedBlock(response, block, traces, deltas) {
        if (!response.this_block)
            return;
        let block_num = response.this_block.block_num;
        if (!(block_num % 100) || block_num >= this.irreversible) {
            if (this.numRows)
                console.log(`    created ${numberWithCommas(this.numRows)} rows`);
            this.numRows = 0;
            console.log(`block ${numberWithCommas(block_num)}`)
        }
        // if (block_num >= 500)
        //     process.exit(1);
        try {
            if (this.head && block_num > this.head + 1)
                throw new Error(`Skipped block(s): head = ${this.head}, received = ${block_num}`);
            let switchForks = this.head && block_num < this.head + 1;
            if (switchForks)
                console.log(`Switch forks: old head = ${this.head}, new head = ${block_num}`);

            if (!this.insertClient)
                this.insertClient = await this.pool.connect();
            await this.insertClient.query('start transaction;');
            this.head = block_num;
            this.irreversible = response.last_irreversible.block_num;
            await this.insertClient.query(`update ${this.schema}.status set head=$1, irreversible=$2;`, [this.head, this.irreversible]);

            if (switchForks) {
                await this.insertClient.query(`delete from ${this.schema}.received_blocks where block_index >= $1;`, [block_num]);
                for (let x of this.sqlTables)
                    await this.insertClient.query(`delete from ${this.schema}."${x[1].name}" where block_index >= $1;`, [block_num]);
            }
            await this.insertClient.query(`insert into ${this.schema}.received_blocks values ($1, $2);`, [block_num, response.this_block.block_id]);

            for (let [_, delta] of deltas) {
                let sqlTable = this.sqlTables.get(delta.name);
                let queries = [];
                this.connection.forEachRow(delta, (present, data) => {
                    let values = sqlTable.fields.map(({ name, type }) => type.convert(data[name]));
                    values[0] = block_num;
                    values[1] = present;
                    queries.push({
                        name: delta.name,
                        text: sqlTable.insert,
                        values
                    });
                });
                this.numRows += queries.length;
                for (let query of queries) {
                    try {
                        await this.insertClient.query(query);
                    } catch (e) {
                        console.log(query);
                        console.log(e);
                        process.exit(1);
                    }
                }
            }
            await this.insertClient.query('commit;');
        } catch (e) {
            console.log(e);
            process.exit(1);
        }
    } // receivedBlock
} // FillPostgress

commander
    .option('-d, --delete-schema', 'Delete schema')
    .option('-c, --create-schema', 'Create schema and tables')
    .option('-i, --irreversible-only', 'Only follow irreversible')
    .option('-s, --schema [name]', 'Schema name', 'chain')
    .option('-a, --socket-address [addr]', 'Socket address', 'ws://localhost:8080/')
    .parse(process.argv);

// const monitor = new Monitor(commander);
const fill = new FillPostgress(commander);
