const WebSocket = require('ws');
const { Serialize } = require('eosjs2');
const fetch = require('node-fetch');
const { TextDecoder, TextEncoder } = require('text-encoding');
const abiAbi = require('./node_modules/eosjs2/src/abi.abi.json');
const pg = require('pg');

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

class Connection {
    constructor({ receivedAbi, receivedBlock }) {
        // todo: initial state (block 1)
        this.last_block_num = 0;
        this.last_requested = 1;
        this.last_processed = 1;
        this.skip_from = -1;
        this.skip_to = 1293913;
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
            throw new Error('oops'); // todo: remove check
        return result;
    }

    send(request) {
        this.ws.send(this.serialize('request', request));
    }

    async onMessage(data) {
        if (!this.abi) {
            this.abi = JSON.parse((new TextDecoder).decode(data));
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

    requestState() {
        this.send(['get_state_request_v0', {}]);
    }

    requestBlocks() {
        for (let block_num = this.last_requested + 1; block_num <= this.last_processed + this.simultaneous && block_num <= this.last_block_num; ++block_num) {
            this.send(['get_block_request_v0', { block_num }]);
            this.last_requested = block_num;
            if (this.last_requested == this.skip_from)
                this.last_requested = this.skip_to;
        }
    }

    async processBlockStates() {
        if (this.inProcessBlockStates)
            return;
        this.inProcessBlockStates = true;
        while (true) {
            let response = this.blockStates.get(this.last_processed + 1);
            if (!response)
                break;
            this.blockStates.delete(response.block_num);
            this.last_processed = response.block_num;
            if (this.last_processed == this.skip_from)
                this.last_processed = this.skip_to;
            const deltas = this.deserialize('table_delta[]', response.deltas);
            await this.receivedBlock(response, deltas);
        }
        this.inProcessBlockStates = false;
        this.requestBlocks();
    }

    get_state_result_v0(response) {
        this.last_block_num = response.last_block_num;
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
                f(row.id, data[1]);
        }
        for (let id of delta.removed)
            f(id, null);
    }

    dumpDelta(delta) {
        this.forEachRow(delta, (id, data) => {
            console.log('   ', id, JSON.stringify(data));
        });
    }
} // Connection

class MonitorTransfers {
    constructor() {
        this.accounts = new Map;
        this.tableIds = new Map;

        this.connection = new Connection({
            receivedAbi: () => this.connection.requestState(),
            receivedBlock: async (message, deltas) => {
                for (let [_, delta] of deltas)
                    if (this[delta.name])
                        this[delta.name](message.block_num, delta);
            }
        });
    }

    getAccountTypes(name) {
        let account = this.accounts.get(name);
        if (!account || !account.abi.length)
            throw new Error('x');
        if (account.types)
            return account.types;
        const abi = abiTypes.get("abi_def").deserialize(new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array: account.abi }));
        account.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), abi);
        return account.types;
    }

    deserializeAccount(name, type, array) {
        const types = this.getAccountTypes(name);
        if (!types)
            throw new Error('a');
        const t = Serialize.getType(types, type);
        if (!t)
            throw new Error('b');
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
        return t.deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: false }));
    }

    account(blockNum, delta) {
        this.connection.forEachRow(delta, (id, data) => {
            if (!data || data.name !== 'eosio.token')
                return;
            this.accounts.set(data.name, { abi: data.abi });
        });
    }

    table_id(blockNum, delta) {
        this.connection.forEachRow(delta, (id, data) => {
            if (!data)
                this.tableIds.delete(id);
            else if (data.code === 'eosio.token' && data.table === 'accounts')
                this.tableIds.set(data.id, data);
        });
    }

    key_value(blockNum, delta) {
        this.connection.forEachRow(delta, (id, data) => {
            if (!data)
                return;
            let tableId = this.tableIds.get(data.t_id);
            if (!tableId)
                return;
            if (tableId.scope !== 'eosio')
                return;
            console.log(`block: ${blockNum} tid: ${data.t_id} id:${data.id} code:${tableId.code} scope:${tableId.scope} table:${tableId.table} table_payer:${tableId.payer} payer:${data.payer} primary_key:${data.primary_key}  ${JSON.stringify(this.deserializeAccount(tableId.code, 'account', data.value))}`);
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
                sqlTable.fields.splice(0, 0, { name: 'block_index', type: { name: 'bigint', convert: x => x } });
                let fieldNames = sqlTable.fields.map(({ name }) => `"${name}"`).join(', ');
                let values = [...Array(sqlTable.fields.length).keys()].map(n => `$${n + 1}`).join(',');
                sqlTable.insert = `insert into ${schema}.${sqlTable.name}(${fieldNames}) values (${values})`;
                let query = `create table ${schema}.${sqlTable.name} (${sqlTable.fields.map(({ name, type }) => `"${name}" ${type.name}`).join(', ')}, primary key(block_index, id));`;
                await this.pool.query(query);
            }

            this.connection.requestState();
        } catch (e) {
            console.log(e);
        }
    }

    async receivedBlock(message, deltas) {
        if (!(message.block_num % 100)) {
            if (this.numRows)
                console.log(`    created ${numberWithCommas(this.numRows)} rows`);
            this.numRows = 0;
            console.log(`block ${numberWithCommas(message.block_num)}`)
        }
        await this.pool.query('start transaction;');
        for (let [_, delta] of deltas) {
            let sqlTable = this.sqlTables.get(delta.name);
            let queries = [];
            this.connection.forEachRow(delta, (id, data) => {
                if (!data)
                    return;
                let values = sqlTable.fields.map(({ name, type }) => type.convert(data[name]));
                values[0] = message.block_num;
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

// let foo = new MonitorTransfers;
let foo = new FillPostgress;
