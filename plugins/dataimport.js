import Bus from '../../bus.js';
import {
  assert,
  chunks,
  getHumanReadableFileSizeString,
  pageAsyncIterator,
  papaPreview,
  papaStream,
  readAsText
} from '../../lib/helpers.js';
import {OpsQueue, BaseDB, TX} from '../../lib/storage.js';
import {getAlternateKeys} from '../../lib/metadata.js';
import FieldComponent from '../../components/field.js';
import {batch} from '../../plugins/fieldupdater.js';


import EntitySelector from '../../components/entityselector.js';
import AttributeSelector from '../../components/attributeselector.js';
import ProgressBar from '../../components/progress.js';


const split = (data, criterion, numCategories = 2) => {
  const categories = new Array(numCategories).fill().map(() => []);

  data.forEach(item => {
    const cat = criterion(item);
    categories[cat].push(item);
  });

  return categories;
};

// Some commonly used aliases
const {Metadata,Service,Utils} = ORBIS.Core;
const WEBAPIURL = Utils.Url.getWebAPIUrl();

const ID = 'ORBIS.JSToolbox.DataImport'; // e.g. "Orbis.JSToolbox.Plugins.Unmanaged.Sonepar.DoSomeStuff"
const PLUGINNAME = 'Data Import'; // e.g. "SPR: Do some Stuff"
const PLUGINICON = 'upload'; // any of the FontAwesome icons (https://fontawesome.com/v4.7.0/icons/)
const PLUGINDESCRIPTION = null;
const PLUGINCSSCLASS = null; // e.g. "orb-plugin-sonepar-dosomestuff"
const PLUGINVERSION = '1.0.3';
const PLUGINAUTHOR = 'Daniel Bruckhaus';
const PLUGINEMAIL = 'daniel.bruckhaus@orbis.de';
const PLUGINKEYWORDS = ["data", "import", "csv", "json", "queue"];

const LARGEFILESIZE = 5 * 1024**2; // 5 MiB

const ETAGREGEX = /^W\/".+"$/;

const FIELDMODES = {
  Map: "Map",
  Ignore: "Ignore",
  ETag: "ETag",
  Static: "Static",
};
const FIELDDEFAULTS = {
  mode: FIELDMODES.Map,
  from: null,
  to: null,
  type: null,
  resolve: false,
  resolveAttribute: null,
  target: null,
  targets: [],
  value: null
};

const LOOKUPTYPES = ["Owner", "Customer", "Lookup"];

let RESOLVECACHE = {};

const RecordImportStatus = {
  New: "New",
  Checked: "Checked",
  Success: "Success",
  Failed: "Failed",
  Enqueued: "Enqueued"
};

const TYPE_STRING = "string";

const identity = x => x;
const REGEX_BOOLEAN = /true|1/i;
const REGEX_INTEGER = /^-?[0-9]+$/;
const REGEX_MOSV = /^\d+(,\d+)*$/;

const HEXDIGIT = `[0-9a-f]`;
const SREGEX_ENTITYLOGICALNAME = `[a-z][a-z0-9_]{0,1022}[a-z0-9]`;

const SREGEX_GUID = `{?${HEXDIGIT}{8}-${HEXDIGIT}{4}-${HEXDIGIT}{4}-${HEXDIGIT}{4}-${HEXDIGIT}{12}}?`;

const REGEX_LOOKUP = new RegExp(`(${SREGEX_ENTITYLOGICALNAME}):(${SREGEX_GUID})`);
const parseBool = b => REGEX_BOOLEAN.test(b);

const parseLookup = ref => {
  const result = REGEX_LOOKUP.exec(ref);
  if(result) {
    const [_, logicalName, id] = result;
    return {logicalName, id};
  }
  else {
    return null;
  }
};


const parseDateTime = dt => {
  if(REGEX_INTEGER.test(dt)) {
    return new Date(parseInt(dt));
  }
  else {
    return moment(dt).toDate();
  }
};

const _OSVByLabel = new Map();
const getOSVByLabel = (label, meta) => {
  if(!_OSVByLabel.has(meta)) {
    const dict = meta.getOptions().reduce((m, {text,value}) => (m.set(text, parseInt(value)), m), new Map());
    _OSVByLabel.set(meta, dict);
  }

  return _OSVByLabel.get(meta).get(label);
};

const parseOSV = (value, meta) => {
  if(value === "" || value === null) {
    return null;
  }
  else if(REGEX_INTEGER.test(value)) {
    return parseInt(value);
  }
  else {
    const osv = getOSVByLabel(value, meta);
    assert(typeof(osv) === "number");
    return osv;
  }
};

const parseMOSV = (value, meta) => {
  if(value === "" || value === null) {
    return null;
  }
  else if(REGEX_MOSV.test(value)) {
    return value;
  }
  else {
    throw new Error(`Cannot parse '${value}' as MultiSelectPicklist value`);
  }
};

const Parsers = {
  String: identity,
  Memo: identity,

  Integer: parseInt,
  Decimal: parseFloat,
  Money: parseFloat,
  Float: parseFloat,

  State: parseOSV,
  Status: parseOSV,
  Picklist: parseOSV,

  MultiSelectPicklist: parseMOSV,

  Customer: parseLookup,
  Lookup: parseLookup,
  Owner: parseLookup,
  Uniqueidentifier: identity,
  Boolean: parseBool,
  DateTime: parseDateTime,
};

const parseValue = (value, attrType, attrMeta) => {
  if(typeof(value) !== TYPE_STRING) return null;

  return Parsers[attrType](value, attrMeta);
};

const COUNTDEFAULTS = {
  New: 0,
  Checked: 0,
  Enqueued: 0,
  Failed: 0,
  Success: 0
};

const REQUESTMODES = {
  AssociateRequest: "AssociateRequest",
  CreateRequest: "CreateRequest",
  UpdateRequest: "UpdateRequest",
  UpsertRequest: "UpsertRequest",
};
const REQUESTMODELABELS = {
  [REQUESTMODES.AssociateRequest]: "Associate (M:N)",
  [REQUESTMODES.CreateRequest]: "Create Only",
  [REQUESTMODES.UpdateRequest]: "Update Only",
  [REQUESTMODES.UpsertRequest]: "Upsert (Create/Update based on key)"
};


const getManyToManyByIntersectEntity = async logicalName => {

  const resp = await fetch(`${WEBAPIURL}/RelationshipDefinitions/Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata?$filter=IntersectEntityName eq '${logicalName}'`);
  const data = await resp.json();
  const results = data.value,
        num = results.length;
  if(num === 1) {
    return results[0];
  }
  else if(num === 0) { throw new Error(`ManyToManyRelationshipMetadata not found for ${logicalName}`); }
  else if(num > 1) { throw new Error(`Duplicate ManyToManyRelationshipMetadatas not found for ${logicalName}`); }
};

const CDN_PAPA_SRC = "https://cdnjs.cloudflare.com/ajax/libs/PapaParse/4.6.3/papaparse.min.js";

const papaWorkerCode = `
let resumeParser, abortParser = null;
onmessage = function(event) {
  const {file, config, resume, abort, workerStepSize} = event.data;
  //console.log("Message", event.data);


  if(file && config) {
    importScripts('${CDN_PAPA_SRC}');

    let steps = 0;
    const page = [];
    const pageSize = workerStepSize;
    Papa.parse(file, {
      ...config,
      worker: false,
      step: (result, parser) => {
        page.push(result);
        if(page.length === pageSize) {
          parser.pause();
          //console.log("Step - Parser paused");
          resumeParser = parser.resume;
          abortParser = parser.abort;
          postMessage({done: false, results: [...page]})
          page.length = 0;
        }
        steps++;
      },
      complete: () => {
        if(page.length) {
          postMessage({done: false, results: [...page]});
        }
        postMessage({done: true, steps});
        close();
      }
    });
  }

  if(resume) {
    if(!resumeParser) { throw new Error("Nothing to resume"); }
    //console.log("Resuming");
    resumeParser();
    //resumeParser = null;
  }
  if(abort) {
    if(!abortParser) { throw new Error("Nothing to abort"); }
    //console.log("Aborting");
    abortParser();
    //abortParser = null;
  }


}`;

const papaWorkerUrl = URL.createObjectURL(new Blob([papaWorkerCode], {type: "text/javascript"}));

const parseInWorker = (file, {step, workerStepSize = 500, ...config} = {}) => new Promise((res, rej) => {
  try {
    const worker = new Worker(papaWorkerUrl);
    worker.onmessage = evt => {
      // console.log("Message", evt.data);
      const {done, results, steps} = evt.data;

      if(results) {
        try {
          const result = results.reduce((m,{data, errors, meta}) => {
            m.data.push(...data);
            m.errors.push(...errors);
            m.meta = meta;
            return m;
          }, {
            data: [],
            errors: [],
            meta: []
          });

          const resp = step(result);

          if(resp && resp.then && resp.catch) {
            // console.log("Step-Promise, add then/catch");
            resp.then(_ => {
              // console.log("Sending 'resume'");
              worker.postMessage({resume: true});
            });
            resp.catch(ex => {
              console.error(ex);
              // console.log("Sending 'abort'");
              worker.postMessage({abort: true});
            });
          }
          else {
            // console.log("Sync-Step; Sending 'resume'");
            worker.postMessage({resume: true});
          }
        }
        catch(ex) {
          console.error(ex);
          // console.log("Sending 'abort'");
          worker.postMessage({abort: true});
        }

      }

      if(done) {
        res({steps});
      }
    };
    worker.onerror = rej;
    worker.postMessage({file, workerStepSize, config});
  }
  catch(ex) {
    rej(ex);
  }
});

class ImportDB extends BaseDB {
  static get VERSION() { return 1; }
  static get DBNAME() { return `${ID}.ImportDB`;}
  static get StoreNames() {
    return  {
      Rows: "Rows",
    };
  }

  constructor() {
    super({
      DbName: ImportDB.DBNAME,
      Version: ImportDB.VERSION,
    });
  }

  get Migrations() {
    return [
      {version: 1, migrate: this.migration_createStores}
    ];
  }

  // version: 1
  migration_createStores(db) {
    const {Rows} = ImportDB.StoreNames;

    db.createObjectStore(Rows, {autoIncrement: true});
    // const tx = db.transaction;
    // const rowStore = tx.objectStore(Rows);
  }

  async AddRows(rows) {
    const {Rows} = ImportDB.StoreNames;
    const tx = this.DB.transaction([Rows], TX.ReadWrite);
    const store = tx.objectStore(Rows);

    for(const row of rows) {
      store.put(row);
    }

    return tx.complete;
  }

  async Clear() {
    const {Rows} = ImportDB.StoreNames;
    const tx = this.DB.transaction([Rows], TX.ReadWrite);
    tx.objectStore(Rows).clear();
    return tx.complete;
  }

  async Count() {
    const {Rows} = ImportDB.StoreNames;
    return await this.getCount(Rows);
  }

  async getMinKey(storeName) {
    const tx = this.DB.transaction([storeName], TX.ReadOnly);
    const store = tx.objectStore(storeName);

    let min = null;
    store.iterateKeyCursor(null, cursor => {
      if (!cursor) return;
      min = cursor.key;
      return;
    });

    await tx.complete;

    return min;
  }

  async *ReadAll({pageSize = 100} = {}) {
    const {Rows} = ImportDB.StoreNames;

    const count = await this.getCount(Rows);
    let from = await this.getMinKey(Rows);
    let to = from + pageSize;
    let read = 0;

    while(read < count) {
      let page = [];

      const tx = this.DB.transaction([Rows], TX.ReadOnly);
      const store = tx.objectStore(Rows);

      let range = IDBKeyRange.bound(from, to, false /*lowerOpen*/, true /* upperOpen*/);

      store.iterateCursor(range, cursor => {
        if (!cursor) return;
        page.push(cursor.value);
        read++;
        cursor.continue();
      });

      await tx.complete;

      if(page.length) {
        yield page;
      }

      from = to;
      to += pageSize;
    }

  }
}

class ArrayReader {
  constructor(array = []) {
    this.array = array;
  }

  Count() {
    return this.array.length;
  }

  async *ReadAll({pageSize = 100} = {}) {
    let page = [];
    for(const item of this.array) {
      page.push(item);

      if(page.length === pageSize) {
        yield [...page];
        page.length = 0;
      }
    }

    if(page.length > 0) yield [...page];
  }
}

class StreamingFileReader{
  constructor(file, type) {
    Object.assign(this, {file, type});
  }

  Count() {
    return -1; // Unkown
  }

  getIterator() {
    switch (this.type) {
      case "csv":
        return papaStream(this.file);
      default:
        throw new Error(`Not Implemented: type ${this.type}`);
    }
  }

  async *ReadAll({
    pageSize = 100
  } = {}) {
    const iterator = this.getIterator();

    for await (const page of pageAsyncIterator(iterator, pageSize)) {
      yield page;
    }
  }
}


export default {
  id: ID,
  version: PLUGINVERSION,
  author: PLUGINAUTHOR,
  email: PLUGINEMAIL,
  name: PLUGINNAME,
  icon: PLUGINICON,
  keywords: PLUGINKEYWORDS,
  //stylesheet: './plugins/jsoneditor.css', // Optional: A stylesheet path, relative to index.html
  component: {
    components: {
      AttributeSelector,
      EntitySelector,
      FieldComponent,
      ProgressBar,
    },
    filters: {
      humanFilesize(bytes, fallback = '', decimals = 2, binary = false ) {
        return typeof(bytes) === "number"
          ? getHumanReadableFileSizeString(bytes, decimals, binary)
          : fallback;
      }
    },
    template: `
      <div class="${PLUGINCSSCLASS} h-full flex flex-col">
        <label>
          <span>Target Entity</span>
          <entity-selector :logicalname="entity" @selection-changed="onEntityChanged"></entity-selector>
        </label>

        <label>
          <span>Import file</span>
          <input type="file" ref="importfile" @change="onFileChanged"/>
          <span>{{ filesize | humanFilesize }}</span>
        </label>
        <div>
          <span>Data Type</span>
          <label class="inline"><input type="radio" name="importfileType" v-model="importfileType" value="csv"/>CSV</label>
          <label class="inline"><input type="radio" name="importfileType" v-model="importfileType" value="json"/>JSON</label>
          <label>
            <input type="checkbox" v-model="options.useImportDb"/>
            <span>Use ImportDB <b v-if="isLargeFile">(recommended for large files)</b></span>
          </label>
        </div>
        <div class="flex flex-row">
          <button class="flex-1" @click="loadFile" :disabled="!canLoadFile">
            <i v-show="loadingFile" class="fa fa-spinner animation-spin"></i>
            <span>Load File</span>
          </button>
          <button class="flex-1" @click="peekFile" :disabled="!canPeek">Peek</button>
        </div>
        <hr class="border border-solid w-full border-grey"/>
        <div v-if="fields.length">
          <div>
            <span>{{records.length}} records loaded.</span>
            <span>New: {{counts.New}} / Checked: {{counts.Checked}} / Enqueued: {{counts.Enqueued}} / Successfully imported: {{counts.Success}} / Failed: {{counts.Failed}} </span>
          </div>
          <div>
            <div>
              <h4>Operation</h4>
              <select v-model="operation" :disabled="isManyToMany">
                <option :value="key" v-for="(lbl, key) in REQUESTMODELABELS">{{lbl}}</option>
              </select>
            </div>
            <div class="contents" v-if="keys.length">
              <h4>Key Mode</h4>
              <select v-model="key">
                <option :value="null">Use Entity Primary Key ({{meta.primaryIdAttribute}})</option>
                <option :value="key" v-for="key in keys">{{key.LogicalName}} ({{key.KeyAttributes}})</option>
              </select>
            </div>
            <h4>Field Mapping</h4>
            <table class="data-table">
              <tr>
                <th>Source (Import File)</th>
                <th>Mode</th>
                <th>Target (CRM Field)</th>
                <th>Options</th>
              </tr>
              <tr v-for="field in fields" class="zebra">
                <td>
                  {{field.from}}
                </td>
                <td>
                  <select v-model="field.mode">
                    <option :value="mode" v-for="mode in FIELDMODES">{{mode}}</option>
                  </select>
                </td>
                <td>
                  <attribute-selector v-show="showAttributeSelector(field)" :logicalname="field.to" :entityname="entity" @selection-changed="onFieldToChanged(field,$event)"></attribute-selector>
                </td>
                <td>
                  <div v-if="isLookup(field) || isManyToMany">
                    <label>
                      <span>Target entity</span>
                      <entity-selector :restrict="field.targets" :logicalname="field.target" @selection-changed="field.target = $event"></entity-selector>
                    </label>
                    <label>
                      <input type="checkbox" v-model="field.resolve"/>
                      <span>Resolve Lookup Value</span>
                    </label>
                    <label v-if="field.target && field.resolve">
                      <span>Resolve by attribute</span>
                      <attribute-selector :logicalname="field.resolveAttribute" :entityname="field.target" @selection-changed="field.resolveAttribute = $event"></attribute-selector>
                    </label>
                  </div>
                  <div v-if="field.mode === FIELDMODES.Static">
                    <field-component v-if="field.type" :entity-metadata="meta" :logicalname="field.to" :show-label="false" :value="field.value" @value-changed="field.value = $event"></field-component>
                  </div>
                </td>
              </tr>
              <tr>
                <td colspan="4">
                  <button @click="addField({mode: FIELDMODES.Static})" title="Add a new import field">
                    <i class="fa fa-plus"></i>
                  </button>
                </td>
              </tr>
            </table>
          </div>
        </div>
        <button @click="prepareData" :disabled="!canPrepareData">Prepare/Validate</button>
        <hr class="border border-solid w-full border-grey"/>
        <div>
          <label>
            <input type="checkbox" v-model="opsqueue.use"/>
            <span>Send update to OpsQueue</span>
          </label>
          <label v-show="opsqueue.use">
            <span>OpsQueue Context</span>
            <input type="text" v-model="opsqueue.context"/>
          </label>
          <label>
            <input type="checkbox" v-model="options.streamFile"/>
            <span>Stream-read data from import file</span>
          </label>
          <div class="flex flex-row">
            <label>
              <input type="checkbox" v-model="options.batchProcessing.use"/>
              <span>Use Batch Processing ("ExecuteMultiple")</span>
            </label>
            <label>
               <span>Parallel requests ("Threads")</span>
               <input type="number" v-model.number="options.batchProcessing.parallel" min="1" max="6"/>
             </label>
             <label>
              <span>Batch Size</span>
              <input type="number" v-model.number="options.batchProcessing.batchsize" min="1" max="1000"/>
            </label>
          </div>
        </div>
        <button @click="importData" :disabled="!canImportData">IMPORT</button>
        <button v-show="importing" @click="cancelled = true" :disabled="cancelled">Cancel</button>

        <progress-bar v-show="importing" :min="0" :max="progress.total" :indeterminate="progress.total < 0" :value="progress.currentIndex" :text="progressText"></progress-bar>
      </div>
    `,
    data () {
      return {
        FIELDMODES,
        REQUESTMODELABELS,
        cancelled: false,
        counts: { ...COUNTDEFAULTS },
        entity: null,
        fields: [],
        fileChangedOn: 0,
        importfileType: null,
        importing: false,
        key: null,
        keys: [],
        loadingFile: false,
        meta: null,
        operation: null,
        opsqueue: {
          use: false,
          context: ID
        },
        options: {
          batchProcessing: {
            use: false,
            batchsize: 50,
            parallel: 4
          },
          streamFile: false,
          useImportDb: false
        },
        progress: {
          currentIndex: -1,
          currentName: null,
          total: -1
        },
        records: [],
        relationship: null,
        validated: false
      };
    },

    // Executed once the plugin component is loaded
    async created () {
      this.data = {
        db: new ImportDB(),
        records: []
      };

      await this.data.db.Open();
    },

    // Executed once the plugin component has been added to the DOM
    mounted  () {

    },

    // Computed properties
    computed: {
      canImportData() {
        return this.canPrepareData
            && !this.importing;
      },
      canPeek() {
        return this.canLoadFile;
      },
      canPrepareData() {
        return this.operation;
      },
      canLoadFile () {
        return this.meta
            && this.importfileType
            && !this.loadingFile
            && this.$refs.importfile && this.$refs.importfile.files.length === 1;
      },

      filesize() {
        if(!this.fileChangedOn) return null;

        return this.$refs.importfile && this.$refs.importfile.files.length ? this.$refs.importfile.files[0].size : null;
      },

      isLargeFile() { return this.filesize > LARGEFILESIZE; },

      isManyToMany() { return this.meta.isIntersect; },

      progressText() {
        const {currentIndex, total} = this.progress;
        if(total > -1) {
          return `${currentIndex} of ${total} processed`;
        }
        else {
          return `${currentIndex} processed (total unknown)`;
        }
      }
    },

    // Methods, Eventhandlers, etc.
    methods: {
      addField(fieldOptions = {}) {
        const field = Object.assign({}, FIELDDEFAULTS, fieldOptions);
        this.fields.push(field);
      },
      countByStatus(status) {
        return 0; // this.records.filter(r => r.status === status).length;
      },

      getDataReader() {
        if(this.options.useImportDb) {
          return this.data.db;
        }
        else if (this.options.streamFile) {
          return new StreamingFileReader(this.$refs.importfile.files[0], this.importfileType);
        }
        else {
          return new ArrayReader(this.data.records);
        }


      },

      async importData() {
        try {
          this.importing = true;
          this.cancelled = false;

          const reader = this.getDataReader();

          //const queue = this.records.filter(r => r.status === RecordImportStatus.Checked);

          const operation = Service.Requests[this.operation];

          this.progress.currentIndex = 0;
          this.progress.total = await reader.Count();

          for await(const page of reader.ReadAll({pageSize: 500})) {
            const pckgs = [];
            for(const record of page) {
              try {
                const pckg = await this.processRecord(record);
                pckgs.push(pckg);
              } catch (e) {
                this.counts.Failed++; // TODO: Don't count failed records twice (on prepare an and in import)
              }
              if(this.cancelled) break;
            }

            if(this.cancelled) break;

            if(this.opsqueue.use) {
              const Context = this.opsqueue.context,
                    Operation = this.operation;

              try {
                const ids = await OpsQueue.pushRange(pckgs.map(pckg => {
                  return {
                    Context,
                    Operation,
                    Data: pckg,
                  };
                }));

                this.counts.Enqueued += pckgs.length;

              }
              catch (ex) {
                const msg = ex.message || "Unkown error";
                // page.forEach(r => {
                //   r.status = RecordImportStatus.Failed;
                //   r.message = msg;
                // });
                //this.counts.Failed += page.length;
              }
              finally {
                this.progress.currentIndex += page.length;
              }
            }
            else if(this.options.batchProcessing.use) {
              const {batchsize, parallel} = this.options.batchProcessing;

              const requests = pckgs.map(p => new operation(p));

              const {promise, cancel:cancelToken} = batch({requests, parallel, batchsize});

              const results = await promise;

              const [successes, failures] = split(results, result => result.isSuccess ? 0 : 1);

              this.counts.Success += successes.length;
              this.counts.Failed += failures.length;
              this.progress.currentIndex += page.length;
            }
            else {
              for(const pckg of pckgs) {
                try {
                  const resp = await Service.execute(new operation(pckg));
                  //record.status = RecordImportStatus.Success;
                  this.counts.Success++;
                } catch (ex) {
                  //record.status = RecordImportStatus.Failed;
                  //record.message = ex.message || "Unkown error";
                  this.counts.Failed++;
                } finally {
                  this.progress.currentIndex++;
                }

                if(this.cancelled) break;
              }
            }
          }

        } catch (e) {
          console.error(e);
          Bus.error({message: "Import Failed. See console for details"});
        } finally {
          this.importing = false;
          this.cancelled = false;
        }

      },

      isAlternateKeyField(attr) {
        return this.keys.some(({KeyAttributes}) => KeyAttributes.includes(attr));
      },

      isLookup(field) {
        return LOOKUPTYPES.includes(field.type);
      },

      async loadFile() {
        try {
          this.loadingFile = true;
          await (this.options.useImportDb ? this.loadFile_importDB : this.loadFile_RAM)();
        } catch (e) {
          console.error(e);
          Bus.error({message: "Loading failed."});
        } finally {
          this.loadingFile = false;
        }

      },

      async loadFile_importDB() {
        const t_start = performance.now();
        await this.data.db.Clear();
        const t_cleared = performance.now();

        console.log(`ImportDB cleared. ${(t_cleared - t_start)/1000}s`);
        const file = this.$refs.importfile.files[0];
        assert(!!file, `No file selected`);

        let fieldsProcessed = false;
        const {steps} = await parseInWorker(file, {
          header: true,
          skipEmptyLines: true,
          step: async result => {
            const {data, errors, meta} = result;
            if(!fieldsProcessed) {
              this.processFields(meta.fields);
              fieldsProcessed = true;
            }
            await this.data.db.AddRows(data);
          }
        });
        const t_loaded = performance.now();
        console.log(`File processed in ${steps} steps. ${(t_loaded - t_cleared)/1000}s`);

      },

      async loadFile_RAM() {
        this.counts = {...COUNTDEFAULTS};
        const file = this.$refs.importfile.files[0];
        //const txt = await readAsText(this.$refs.importfile.files[0]);
        if(this.importfileType === "csv") {
          const rows = [];
          let fieldsProcessed = false;
          const parseResults = Papa.parse(file, {
            worker: false,
            header: true,
            skipEmptyLines: true,
            step: (result, parser) => {
              const {data, errors, meta} = result;
              rows.push(...data);

              if(!fieldsProcessed) {
                this.processFields(meta.fields);
                fieldsProcessed = true;
              }
            },
            complete: () => {
              this.data.records = rows;
              //   .map(raw => {
              //   return {
              //     raw,
              //     pckg: null,
              //     message: null,
              //     status: RecordImportStatus.New
              //   };
              // });
              this.counts.New = rows.length;
            }
          });

        }
        else if(this.importfileType === "json") {
          debugger;
          const json = await readAsText(file);
          const rows = JSON.parse(json);
          this.data.records = rows;
          this.counts.New = rows.length;
          if(rows.length) {
            const fields = Object.keys(rows[0]);
            this.processFields(fields);
          }
        }
        else {
          Bus.error({message: "Not yet implemented :-("});
          return;
        }
      },

      async onEntityChanged(newEntity) {
        try {
          this.fields = [];
          this.key = null;

          const [meta,keys] = await Promise.all([
            Metadata.getEntity(newEntity),
            getAlternateKeys(newEntity)
          ]);
          this.meta = meta;
          this.keys = keys;
          this.entity = newEntity;


          if(meta.isIntersect) {
            this.relationship = await getManyToManyByIntersectEntity(this.entity);
            this.operation = REQUESTMODES.AssociateRequest;
          }
          else {
            this.relationship = null;
          }
        } catch (e) {
          Bus.error({message: `Failed to load entity metadata for ${newEntity}`});
        }
      },

      async onFieldToChanged(field, newField) {
        field.to = newField;
        const attrMeta = this.meta.attributes[newField] || null;
        field.type = attrMeta ? attrMeta.type : null;
        if(this.isLookup(field)) {
          field.targets = attrMeta.targets;
          if(field.targets.length === 1) {
            field.target = field.targets[0];
          }
        }
        else if (this.isManyToMany && this.relationship) {
          const {
            Entity1LogicalName,
            Entity2LogicalName,
            Entity1IntersectAttribute,
            Entity2IntersectAttribute
          } = this.relationship;

          const map = {
            [Entity1IntersectAttribute]: Entity1LogicalName,
            [Entity2IntersectAttribute]: Entity2LogicalName,
          };

          field.targets = [Entity1LogicalName, Entity2LogicalName];
          field.target = map[field.from] || null;
        }
      },

      onFileChanged() {
        this.fileChangedOn = performance.now();
      },

      parseInWorker(...args) {
        return parseInWorker(...args);
      },

      async peekFile() {
        const file = this.$refs.importfile.files[0];
        const {importfileType:type} = this;

        if (type === "csv") {
          const {meta} = await papaPreview(file, {
            preview: 10,
            header: true
          });
          this.processFields(meta.fields);
        }
        else if(type === "json") {
          Bus.error({message: "Not implemented: Peek JSON file"});
          return;
        }
        else {
          throw new Error(`Unknown import file type: ${type}`);
        }
      },

      async prepareData() {
        debugger;
        this.validated = false;

        const reader = this.getDataReader();


        for await(const page of reader.ReadAll({pageSize: 250})) {
          for(const record of page) {
            try {
              const pckg = await this.processRecord(record);
              this.counts.Checked ++;
            } catch (e) {
              this.counts.Failed ++;
            }
          }
        }

        this.validated = true;
        return;

        // eslint-disable-next-line no-unreachable
        try {
          for(const record of this.records) {
            this.counts.New--;
            const {raw} = record;
            const pckg = {
              logicalName: this.entity,
              id: this.operation === REQUESTMODES.UpdateRequest
                  ? null
                  : this.key
                    ? {}                          // default to an empty alternate key object
                    : Utils.GUID.newGuidString(), // default to a new Guid for Create/Upsert
              etag: null,
              entity: {}
            };

            try {
              for(const field of this.fields) {
                const {mode, from, to, type} = field;
                if(mode === FIELDMODES.Ignore){
                  continue;
                }
                else if(mode === FIELDMODES.ETag) {
                  const tag = raw[from];

                  if(tag === null || tag === "") continue;

                  if(ETAGREGEX.test(tag)) {
                    pckg.etag = raw[from];
                  }
                  else {
                    throw new Error(`Invalid ETag: ${tag}`);
                  }
                }
                else if(mode === FIELDMODES.Map) {
                  const toMeta = this.meta.attributes[to];

                  if(to === this.meta.primaryIdAttribute) {
                    pckg.id = raw[from] || pckg.id;
                    if(this.operation === REQUESTMODES.CreateRequest) {
                      pckg.entity[to] = pckg.id;
                    }
                  }
                  else if(this.key && this.key.KeyAttributes.includes(to)) {
                    const crmVal = parseValue(raw[from], type, toMeta);
                    pckg.id[to] =  crmVal;
                    pckg.entity[to] = crmVal;
                  }
                  else if(this.isLookup(field)) {
                    const {target, resolve, resolveAttribute} = field;
                    let id = null;

                    if(resolve) {
                      id = await this.resolve(target, resolveAttribute, raw[from]);
                    }
                    else {
                      id = raw[from];
                    }

                    pckg.entity[to] = {
                      logicalName: target,
                      id
                    };
                  }
                  else {
                    pckg.entity[to] = parseValue(raw[from], type, toMeta);
                  }
                }
                else {
                  throw new Error(`Invalid/Unknown field mode: ${mode}`);
                }


              }

              record.pckg = pckg;
              record.status = RecordImportStatus.Checked;
              this.counts.Checked++;
            } catch (ex) {
              record.status = RecordImportStatus.Failed;
              record.message = ex.message || "An unkown error occured.";
              this.counts.Failed++;
            }
          }


          this.validated = true;
        } catch (ex) {
          console.error(ex);
          Bus.error({message: "validation failed. See console for details"});
        }
      },

      processFields(fields) {
        this.fields = fields.map(from => {
          const field = Object.assign({}, FIELDDEFAULTS, {from});

          // Auto-Detect ETag:
          if(from === "etag") {
            field.mode = FIELDMODES.ETag;
            this.onFieldToChanged(field, "etag");
          }
          // Initialize 'to' with 'from', if found
          else if(this.meta.attributes[from]) {
            this.onFieldToChanged(field, from);
          }

          return field;
        });
      },

      async processRecord(raw) {
        if(this.operation === REQUESTMODES.AssociateRequest) {
          const {
            SchemaName,
            Entity1LogicalName,
            Entity2LogicalName
          } = this.relationship;

          const pckg = {
            logicalNameA: Entity1LogicalName,
            idA: null,
            logicalNameB: Entity2LogicalName,
            idB: null,
            relationName: SchemaName
          };

          for(const field of this.fields) {
            const {mode, from, to, target} = field;

            if(FIELDMODES.Map === mode) {
              switch (target) {
                case Entity1LogicalName:
                  pckg.idA = raw[from];
                  break;
                case Entity2LogicalName:
                  pckg.idB = raw[from];
                  break;
                default:
                  throw new Error(`Cannot map ${from} to ${to} in an AssociateRequest`);
              }
            }
            else if(FIELDMODES.Ignore === mode) {
              continue;
            }
            else {
              throw new Error(`Unsuppored field mode ${mode} for AssociateRequest`);
            }
          }

          return pckg;
        }
        else {
          const pckg = {
            logicalName: this.entity,
            id: this.operation === REQUESTMODES.UpdateRequest
                ? null
                : this.key
                  ? {}                          // default to an empty alternate key object
                  : Utils.GUID.newGuidString(), // default to a new Guid for Create/Upsert
            etag: null,
            entity: {}
          };

          for(const field of this.fields) {
            const {mode, from, to, type} = field;
            if(mode === FIELDMODES.Ignore){
              continue;
            }
            else if(mode === FIELDMODES.ETag) {
              const tag = raw[from];

              if(tag === null || tag === "") continue;

              if(ETAGREGEX.test(tag)) {
                pckg.etag = raw[from];
              }
              else {
                throw new Error(`Invalid ETag: ${tag}`);
              }
            }
            else if(mode === FIELDMODES.Map) {
              const toMeta = this.meta.attributes[to];

              if(to === this.meta.primaryIdAttribute) {
                pckg.id = raw[from] || pckg.id;
                if(this.operation === REQUESTMODES.CreateRequest) {
                  pckg.entity[to] = pckg.id;
                }
              }
              else if(this.key && this.key.KeyAttributes.includes(to)) {
                const crmVal = parseValue(raw[from], type, toMeta);
                pckg.id[to] =  crmVal;
                pckg.entity[to] = crmVal;
              }
              else if(this.isLookup(field)) {
                const {target, resolve, resolveAttribute} = field;
                let id = null;

                if(resolve) {
                  id = await this.resolve(target, resolveAttribute, raw[from]);
                }
                else {
                  id = raw[from];
                }

                pckg.entity[to] = {
                  logicalName: target,
                  id
                };
              }
              else {
                pckg.entity[to] = parseValue(raw[from], type, toMeta);
              }
            }
            else if (mode === FIELDMODES.Static) {
              pckg.entity[to] = field.value;
            }
            else {
              throw new Error(`Invalid/Unknown field mode: ${mode}`);
            }


          }

          return pckg;
        }
      },

      async resolve(entity, attr, value) {
        if(!RESOLVECACHE[entity]) RESOLVECACHE[entity] = {};

        const ECACHE = RESOLVECACHE[entity];

        if(!ECACHE[attr]) ECACHE[attr] = new Map();

        const ACACHE = ECACHE[attr];

        if(ACACHE.has(value)) {
          const {value:id, valid, message} = ACACHE.get(value);
          if(valid) {
            return id;
          }
          else {
            throw new Error(message);
          }
        }
        else {
          const meta = await Metadata.getEntity(entity);
          const attrMeta = meta.attributes[attr];

          let filter = null;

          switch (attrMeta.type) {
            case "String":
            case "Memo":
              filter = `${attr} eq '${value}'`;
              break;
            default:
              throw new Error(`Not Implememted: Resolve attribute type ${attrMeta.type}`);
          }

          const pk = meta.primaryIdAttribute;

          const params = `$select=${pk}&$filter=${filter}&$top=2`;

          const resp = await Service.retrieveMultiple(entity, params);

          if(resp.data.values.length === 1) {
            const id = resp.data.values[0][pk];

            ACACHE.set(value, {
              value: id,
              valid: true
            });

            return id;
          }
          else if(resp.data.values.length === 0) {
            const message = `Not Found: ${entity}.${attr} = ${value}`;
            ACACHE.set(value, {
              value: null,
              valid: false,
              message
            });
            throw new Error(message);
          }
          else {
            const message = `Duplicates Found: ${entity}.${attr} = ${value}`;
            ACACHE.set(value, {
              value: null,
              valid: false,
              message
            });
            throw new Error(message);
          }
        }

      },

      showAttributeSelector(field) {
        const {Map, Static} = FIELDMODES;
        return [Map, Static].includes(field.mode);
      }

    }

  }
};
