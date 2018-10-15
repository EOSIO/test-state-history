const WebSocket = require('ws');
const { Serialize } = require('eosjs2');
const fetch = require('node-fetch');
const { TextDecoder, TextEncoder } = require('text-encoding');
const abiAbi = require('./node_modules/eosjs2/src/abi.abi.json');
const pg = require('pg');
const zlib = require('zlib');

const schema = 'chain';

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
    bytes: { name: 'bytea', convert: x => Serialize.arrayToHex },
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
    constructor({ receivedAbi, receivedBlock }) {
        this.end_block_num = 0;
        this.lastRequested = 1;
        this.lastProcessed = 1;
        this.skipFrom = -1;
        this.skipTo = 18000000;
        this.simultaneous = 10;
        this.inProcessBlockStates = false;
        this.receivedAbi = receivedAbi;
        this.receivedBlock = receivedBlock;

        this.abi = null;
        this.types = null;
        this.tables = new Map;
        this.blockStates = new Map;

        this.ws = new WebSocket('ws://localhost:8080/', { perMessageDeflate: false });
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
                return "...";
            return v;
        }, 4)
    }

    send(request) {
        this.ws.send(this.serialize('request', request));
    }

    async onMessage(data) {
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
    }

    requestStatus() {
        this.send(['get_status_request_v0', {}]);
    }

    requestBlocks() {
        for (let block_num = this.lastRequested + 1; block_num <= this.lastProcessed + this.simultaneous && block_num < this.end_block_num; ++block_num) {
            this.send(['get_block_request_v0', { block_num }]);
            this.lastRequested = block_num;
            if (this.lastRequested == this.skipFrom) {
                this.lastRequested = this.skipTo;
                break;
            }
        }
    }

    async processBlockStates() {
        if (this.inProcessBlockStates)
            return;
        this.inProcessBlockStates = true;
        while (true) {
            let response = this.blockStates.get(this.lastProcessed + 1);
            if (!response)
                break;
            this.blockStates.delete(response.block_num);
            this.lastProcessed = response.block_num;
            if (this.lastProcessed == this.skipFrom)
                this.lastProcessed = this.skipTo;
            let block, traces = [], deltas = [];
            if (response.block.length)
                block = this.deserialize('signed_block', response.block);
            if (response.traces.length)
                traces = this.deserialize('transaction_trace[]', response.traces);
            if (response.deltas.length)
                deltas = this.deserialize('table_delta[]', response.deltas);
            await this.receivedBlock(response, block, traces, deltas);
        }
        this.inProcessBlockStates = false;
        this.requestBlocks();
    }

    get_status_result_v0(response) {
        this.end_block_num = response.head_block_num + 1;
        this.requestBlocks();
    }

    get_block_result_v0(response) {
        this.blockStates.set(response.block_num, response);
        this.processBlockStates();
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

    dumpDelta(delta) {
        this.forEachRow(delta, (present, data) => {
            console.log('   ', present, JSON.stringify(data));
        });
    }
} // Connection

class MonitorTransfers {
    constructor() {
        this.accounts = new Map;
        this.tableIds = new Map;

        this.connection = new Connection({
            receivedAbi: () => this.connection.requestStatus(),
            receivedBlock: async (response, block, traces, deltas) => {
                if (!(response.block_num % 100))
                    console.log(`block ${numberWithCommas(response.block_num)}`)
                // if (block)
                //     console.log(this.connection.toJsonUnpackTransaction(block));
                // if (traces.length)
                //     console.log(toJsonNoBin(traces));
                for (let [_, delta] of deltas)
                    if (this[delta.name])
                        this[delta.name](response.block_num, delta);
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
} // MonitorTransfers

class FillPostgress {
    constructor() {
        this.pool = new pg.Pool;
        this.sqlTables = new Map;
        this.numRows = 0;

        this.connection = new Connection({
            receivedAbi: () => this.createDatabase(),
            receivedBlock: this.receivedBlock.bind(this),
        });
    }

    async createDatabase() {
        try {
            try {
                await this.pool.query(`drop schema ${schema} cascade`);
            } catch (e) {
            }
            await this.pool.query(`create schema ${schema}`);

            for (let abiTable of this.connection.abi.tables) {
                const type = Serialize.getType(this.connection.types, abiTable.type).fields[0].type;
                const sqlTable = { name: abiTable.name, fields: [], insert: '' };
                this.sqlTables.set(sqlTable.name, sqlTable);
                for (let field of type.fields) {
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
                sqlTable.insert = `insert into ${schema}.${sqlTable.name}(${fieldNames}) values (${values})`;
                let pk = '"block_index"' + abiTable.key_names.map(x => ',"' + x + '"').join('');
                let query = `create table ${schema}.${sqlTable.name} (${sqlTable.fields.map(({ name, type }) => `"${name}" ${type.name}`).join(', ')}, primary key(${pk}));`;

                await this.pool.query(query);
            }

            this.connection.requestStatus();
        } catch (e) {
            console.log(e);
        }
    }

    async receivedBlock(response, block, traces, deltas) {
        if (!(response.block_num % 100)) {
            if (this.numRows)
                console.log(`    created ${numberWithCommas(this.numRows)} rows`);
            this.numRows = 0;
            console.log(`block ${numberWithCommas(response.block_num)}`)
        }
        await this.pool.query('start transaction;');
        for (let [_, delta] of deltas) {
            let sqlTable = this.sqlTables.get(delta.name);
            let queries = [];
            this.connection.forEachRow(delta, (present, data) => {
                let values = sqlTable.fields.map(({ name, type }) => type.convert(data[name]));
                values[0] = response.block_num;
                values[1] = present;
                queries.push([sqlTable.insert, values]);
            });
            for (let [query, value] of queries) {
                try {
                    await this.pool.query(query, value);
                    this.numRows += queries.length;
                } catch (e) {
                    console.log(query, value);
                    console.log(e);
                }
            }
        }
        await this.pool.query('commit;');
    }
} // FillPostgress

let foo = new MonitorTransfers;
// let foo = new FillPostgress;
