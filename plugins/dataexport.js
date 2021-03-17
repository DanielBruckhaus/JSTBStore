import Bus from '../../bus.js';
import {Google} from '../../bus.js';
import {loadScript, pageAll, retrieveAll} from '../../lib/helpers.js';

import EntitySelector from '../../components/entityselector.js';
import AttributeSelector from '../../components/attributeselector.js';
import ViewSelector from '../../components/viewselector.js';

const {Metadata,Service} = ORBIS.Core;

const parser = new DOMParser();

const csv = (data, {fields, ...papaOpts}) => Papa.unparse({data, fields}, {...papaOpts});

const ATTRIBUTETYPEBLACKLIST = ["Virtual"];
const MEASURE_PREFIX = "ORBIS.JSToolbox.Performance.DataExport";

const MarkStart = name => performance.mark(`${MEASURE_PREFIX}.Start.${name}`);
const MeasureStart = name => performance.mark(`${MEASURE_PREFIX}.Start.${name}`);
const MarkEnd = name => performance.mark(`${MEASURE_PREFIX}.End.${name}`);
const Measure = name => performance.measure(`${MEASURE_PREFIX}.Measure.${name}`, `${MEASURE_PREFIX}.Start.${name}`, `${MEASURE_PREFIX}.End.${name}`);
const MeasureEnd = name => (MarkEnd(name), Measure(name));
const Measures = prefix => performance.getEntriesByType("measure").filter(({name}) => name.startsWith(prefix));

const getNodeAttributes = (node, ...attrs) => attrs.reduce((m,a) => (m[a] = node.getAttribute(a), m), {});

const TARGETS = {
  File: "File",
  GDrive: "GDrive",
  SaveAs: "SaveAs"
};
const TARGETLABELS = {
  [TARGETS.File]: "File Download",
  [TARGETS.GDrive]: "Google Drive",
  [TARGETS.SaveAs]: "Save As (Experimental!)",
};

class SaveAsTarget {
  constructor(ENTITY) {
    this.entity = ENTITY;
    this.encoder = new TextEncoder();
    this.newline = '\n';
    this.delimiter = ';';
  }

  async Init({
    filename = this.entity
  } = {}) {
    const opts = {
      type: 'saveFile',
      accepts: [
        {
          description: `.csv (Comma-Seperated Values)`,
          extensions: ["csv"],
          mimeTypes: ["text/csv"]
        }
      ]
    };

    this.handle = await window.chooseFileSystemEntries(opts);
    // Create a writer (request permission if necessary).
    this.writer = await this.handle.createWriter();
    // Make sure we start with an empty file
    await this.writer.truncate(0);

    this.position = 0;
  }

  getBlob(text) {
    const data = this.encoder.encode(text);
    return new Blob([data]);
  }

  async Header(columns) {
    const {newline} = this;
    const fakeFirstRow = Object.fromEntries(columns.map(f => ([f,f])));

    this.columns = columns;

    const csv = this.getCsv([fakeFirstRow]);
    const header = csv.split(this.newline)[0];
    await this.writeLine(header);
  }

  getCsv(rows = []) {
    const {newline, delimiter, columns} = this;
    const csv = Papa.unparse(rows, {columns, delimiter, newline});
    return csv;
  }

  async write(text) {
    const blob = this.getBlob(text);

    await this.writer.write(this.position, blob);

    this.position += blob.size;
  }

  async writeLine(text) {
    await this.write(text + this.newline);
  }

  async AddRange(rows = []) {
    const csv = this.getCsv(rows);
    await this.writeLine(csv);
  }

  async Finalize() {
    // Close the file and write the contents to disk
    await this.writer.close();
  }
}

class GDriveExportTarget {
  constructor(ENTITY) {

  }

  async Init({
    filename = `GDrive Export - ${this.ENTITY}`
  } = {}) {
    this.spreadsheet = await Google.Sheets.Create({title: filename});

    console.log(`New Spreadsheet URL: ${this.spreadsheet.spreadsheetUrl}`);
    this.sheetName = this.spreadsheet.sheets[0].properties.title;
    this.sheetId = 0;
  }

  get spreadsheetId () { return this.spreadsheet.spreadsheetId; }
  get Url () { return this.spreadsheet.spreadsheetUrl; }

  async Header(fields) {
    this.fields = fields;
    const {spreadsheetId, sheetName:range} = this;
    const headerResp = await Google.Sheets.Append({
     spreadsheetId,
     range,
     values: [
       fields,
     ]
   });
  }

  async AddRange(rows = []) {
    const sheetData = rows.map(record => this.fields.map(f => record[f] || ""));

    const {spreadsheetId, sheetName:range} = this;

    const dataResp = await Google.Sheets.Append({
      spreadsheetId,
      range,
      values: sheetData
    });
  }

  async Finalize() {
    // Nothing to to here...
  }
}



const exportData = ({
  target = null,
  logicalName = null,
  fetchXml = null,
  params = null,
  fields = null,
  fieldTypes = null,
  format = "csv",
  exportFormattedValues = true,
  includeEtag = true,
  filename = null,
  zipResult = true,
  download = true,
  staticData = null,
  jsonIndent = 2,
  pageTick = null,
} = {}) => {
  const token = {cancelled: false};
  const cancel = () => token.cancelled = true;

  if(target) {
    download = false;
  }

  const promise = (async _ => {

    MeasureStart("exportData");

    const startAt = new moment();

    if(!filename) {
      filename = `DataExport ${logicalName} ${startAt.format("YYYYMMDD_HHmm")}`;
    }

    let txtMime = null;
    let extension = null;
    switch(format) {
      case "csv":
        txtMime = "text/csv";
        extension = "csv";
        break;
      case "json":
        txtMime = "application/json";
        extension = "json";
        break;
      case "xlsx":
        txtMime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        extension = "xlsx";
        break;
      default:
        throw new Error(`Invalid/Unrecognized format: '${format}'`);
    }

    const meta = await Metadata.getEntity(logicalName);
    const pk = meta.primaryIdAttribute;

    if(!fields) {
      fields = Object.values(meta.attributes)
        .filter(a => !ATTRIBUTETYPEBLACKLIST.includes(a.type))
        .map(a => a.name)
        .sort();
    }

    if(!fieldTypes) {
      fieldTypes = fields.reduce((m,name) => (m[name] = meta.attributes[name] ? meta.attributes[name].type : "Unknown", m), {});
    }

    let dataProvider = null;

    if(fetchXml) {
      dataProvider = pageAll(fetchXml);
    }
    else {
      dataProvider = retrieveAll(logicalName, params);
    }

    const textValue = (record, attr, type) => {
      const formatted = record["@meta"].formattedValues;
      const value = record[attr];
      if(value === null || typeof(value) === "undefined") {
        return "";
      }

      if(exportFormattedValues) {
        const formattedValue = formatted[attr];

        if(formattedValue) return formattedValue;
      }

      switch(type) {
        case "DateTime":
          return value.toISOString();
        case "Lookup":
        case "Owner":
        case "Customer":
          return exportFormattedValues ? value.name : value.id;
        default:
          return value.toString();
      }

    };

    const RESULTS = [];
    let RowsExported = 0;

    const columns = includeEtag ? ["etag"].concat(fields) : fields;

    if(target) {
      await target.Init({filename});

      if(target.Url) {
        window.open(target.Url, "_blank", "noopener");
      }

      await target.Header(columns);
    }

    MeasureStart('loadData');

    for await(const page of dataProvider) {
      const pageResults = page.map(record => {
        const result = fields.reduce((m,f) => (m[f] = textValue(record, f, fieldTypes[f]), m), {});
        if(includeEtag) {
          result.etag = record["@meta"].etag;
        }
        if(staticData) {
          Object.assign(result, staticData);
        }
        return result;
      });

      if(target) {
        await target.AddRange(pageResults);
      }
      else {
        RESULTS.push(...pageResults);
      }

      RowsExported += pageResults.length;

      if(typeof(pageTick) === "function") {
        pageTick({total: RowsExported, page: pageResults.length});
      }

      if(token.cancelled) break;

    }

    if(target) {
      await target.Finalize();
    }

    MeasureEnd('loadData');

    if(token.cancelled) {
      return {cancelled: true};
    }

    let TXTDATA = null;

    MeasureStart('stringify');



    let BLOB = null;
    let canZip = true;

    if("csv" === format) {
      TXTDATA = csv(RESULTS, {
        fields: columns,
        delimiter: ";"
      });
    }
    else if ("json" === format) {
      TXTDATA = JSON.stringify(RESULTS, null, jsonIndent);
    }
    else if("xlsx" === format) {
      debugger;
      const arrayOfArrays = RESULTS.map(record => columns.map(col => record[col]));
      arrayOfArrays.unshift(columns);
      const sheet = XLSX.utils.aoa_to_sheet(arrayOfArrays);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook,sheet,logicalName);
      const buffer = XLSX.write(workbook, {type: "array", bookType: "xlsx"});
      BLOB = new Blob([buffer], {type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"});
      canZip = false;
    }


    MeasureEnd("stringify");



    let fileextension = extension;
    if(zipResult && canZip) {
      MeasureStart("zip");
      const zip = new JSZip();
      zip.file(`${filename}.${extension}`, TXTDATA);
      BLOB = await zip.generateAsync({
        type:"blob",
        compression: "DEFLATE",
        compressionOptions: {
            level: 9
        }
      });
      MeasureEnd("zip");
      fileextension = "zip";
    }
    else if(BLOB === null) {
      BLOB = new Blob([TXTDATA], {
        type: txtMime
      });
    }

    if(download) {
      saveAs(BLOB, `${filename}.${fileextension}`);
    }

    MeasureEnd("exportData");
    return {
      data: download ? null : TXTDATA,
      dataLength: TXTDATA ? TXTDATA.length : -1,
      blobSize: BLOB.size,
      blob: download ? null : BLOB,
      measures: Measures(MEASURE_PREFIX)
    };
  })();

  return {cancel, promise};

};

const ID = 'JSToolbox.Unmanaged.DataExport'; // e.g. "Orbis.JSToolbox.Plugins.Unmanaged.Sonepar.DoSomeStuff"
const PLUGINNAME = 'Data Export'; // e.g. "SPR: Do some Stuff"
const PLUGINICON = 'download'; // any of the FontAwesome icons (https://fontawesome.com/v4.7.0/icons/)
const PLUGINDESCRIPTION = 'Export data as CSV or JSON';
const PLUGINCSSCLASS = null; // e.g. "orb-plugin-sonepar-dosomestuff"
const PLUGINVERSION = '0.9';
const PLUGINAUTHOR = 'Daniel Bruckhaus';
const PLUGINEMAIL = 'daniel.bruckhaus@orbis.de';
const PLUGINKEYWORDS = ["data", "export", "csv", "json", "download"];


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
      ViewSelector
    },
    template: `
      <div class="${PLUGINCSSCLASS} h-full flex flex-col overflow-auto">
        <div>
          <h4>Entity to export</h4>
          <entity-selector :logicalname="entity" @selection-changed="onEntityChanged"></entity-selector>
        </div>
        <div>
          <h4>Source Mode</h4>
          <select v-model="sourceMode">
            <option value="none">No filter (export all records)</option>
            <option value="odata">OData filter</option>
            <option value="view">View</option>
            <option value="fetchxml">FetchXml</option>
          </select>
        </div>
        <div v-if="'fetchxml' === sourceMode">
          <h4>FetchXml</h4>
          <textarea rows="20" v-model="customFetch" class="w-full" @change="onCustomFetchChanged"></textarea>
        </div>
        <div v-if="'view' === sourceMode">
          <h4>Select a view</h4>
          <view-selector v-if="entity" :entity="entity" @selection-changed="onViewChanged"></view-selector>
        </div>
        <div v-if="'odata' === sourceMode">
          <label>
            <span>OData Filter</span>
            <input type="text" v-model="filter"/>
          </label>
        </div>
        <div v-if="entity">
          <h4>Fields to export</h4>
          <table class="data-table">
            <tr>
              <th>Display Name</th>
              <th>Logical Name</th>
              <th>Entity</th>
              <th>Alias</th>
              <th>Type</th>
              <th>&nbsp;</th>
            </tr>
            <tr v-for="(f,i) in fields" class="data-row">
              <td>{{f.displayName}}</td>
              <td>{{f.name}}</td>
              <td>{{f.entity}}</td>
              <td>{{f.alias}}</td>
              <td>{{f.type}}</td>
              <td>
                <button @click="moveFieldTo(f, i - 1)" :disabled="i < 1">
                  <i class="fa fa-arrow-up"></i>
                </button>
                <button @click="moveFieldTo(f, i + 1)" :disabled="i >= fields.length - 1">
                  <i class="fa fa-arrow-down"></i>
                </button>
                <button @click="removeField(f)" title="Remove field">
                  <i class="fa fa-trash"></i>
                </button>
              </td>
            </tr>
            <tr v-if="canAddFields">
              <td colspan="6">
                <span>Select</span>
                <attribute-selector :logicalname="newField" :restrict="addableAttributes" :entityname="entity" @selection-changed="onFieldChanged"></attribute-selector>
              </td>
            </tr>
          </table>
        </div>
        <div>
          <h4>Options</h4>
          <label>
            <span>Target</span>
            <select v-model="target">
              <option v-for="(label,name) in TARGETLABELS" :value="name">{{label}}</option>
            </select>
          </label>
          <label>
            <span>Export Format</span>
            <select v-model="format">
              <option value="csv">CSV</option>
              <option value="json">JSON</option>
              <option value="xlsx">XLSX</option>
            </select>
          </label>
          <label>
            <input type="checkbox" v-model="zipResult"/>
            <span>ZIP Export File</span>
          </label>
          <label>
            <input type="checkbox" v-model="exportFormattedValues"/>
            <span>Export Formatted Values (Labels, Times, Numbers etc.)</span>
          </label>
          <label>
            <input type="checkbox" v-model="includeEtag"/>
            <span>Include ETags</span>
          </label>

        </div>
        <div>
          <button @click="startExport" :disabled="!canStartExport">
            <i class="fa fa-download"></i>
            <span>Export</span>
          </button>
        </div>
        <div v-if="exporting" class="absolute bg-translucent flex flex-col font-bold items-center justify-center pin">
          <span class="text-3xl text-theme-navbarbackgroundcolor">
            <i class="fa fa-spinner animation-spin"></i>
          </span>
          <span>Exporting: {{exportCount}} records...</span>
          <button :disabled="!canCancelExport" @click="cancelExport">
            <i class="fa fa-stop"></i>
            <span>Cancel Export</span>
          </button>
        </div>
      </div>
    `,
    data () {
      return {
        TARGETS, TARGETLABELS,
        cancelled: false,
        customFetch: null,
        customFetchValid: false,
        entity: null,
        exporting: false,
        exportCount: 0,
        exportFormattedValues: false,
        filter: null,
        format: "csv",
        meta: null,
        fields: [],
        includeEtag: true,
        newField: null,
        sourceMode: null,
        target: TARGETS.File,
        view: null,
        zipResult: true
      };
    },

    // Executed once the plugin component is loaded
    async created () {
      this.data = {}; // Non-reactive data goes there!

      await Promise.all([
        loadScript('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.13.5/xlsx.full.min.js'),
        loadScript(`https://cdnjs.cloudflare.com/ajax/libs/FileSaver.js/1.3.8/FileSaver.min.js`),
        loadScript(`https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.5/jszip.min.js`)
      ]);
    },

    // Executed once the plugin component has been added to the DOM
    mounted  () {

    },

    // Computed properties
    computed: {
      addableAttributes() {
        if(this.meta) {
          const existing = this.fields.map(attr => attr.name);
          return Object.values(this.meta.attributes)
            .filter(attr => !existing.includes(attr.name))
            .map(attr => attr.name);
        }
        else {
          return [];
        }
      },

      canAddFields() {
        return ["none", "odata"].includes(this.sourceMode);
      },

      canCancelExport() {
        return this.exporting && !this.cancelled;
      },

      canStartExport() {
        return !this.exporting
            && this.entity
            && this.fields.length > 0
            && this.format
            && this.target;
      },

      fetchXml() {
        switch (this.sourceMode) {
          case "view":
            return this.view ? this.view.fetchxml : null;
          case "fetchxml":
            return this.customFetch;
          default:
            return null;
        }
      }
    },

    // Methods, Eventhandlers, etc.
    methods: {
      cancelExport() {
        if(this.data.cancel) {
          this.data.cancel();
          this.data.cancel = null;
          this.cancelled = true;
        }
        else {
          Bus.warn({message: "Unable to cancel export."});
        }
      },

      moveFieldTo(field, toIndex) {
        const fromIndex = this.fields.indexOf(field);
        if(fromIndex > -1) {
          this.fields.splice(fromIndex, 1);
          const to = toIndex < fromIndex ? toIndex : toIndex - 1;
          this.fields.splice(to, 0, field);
        }
      },

      async startExport() {
        try {
          this.exporting = true;
          this.exportCount = 0;

          const filter = this.filter || `${this.meta.primaryIdAttribute} ne null`;

          const fields = this.fields.map(({name, alias}) => alias || name);
          const fieldTypes = this.fields.reduce((m,{name,alias,type}) => (m[alias||name] = type, m), {});

          const select = this.fields.map(attr => {
            switch (attr.type) {
              case "Lookup":
              case "Customer":
              case "Owner":
                return `_${attr.name}_value`;
              default:
              return attr.name;
            }
          }).join(',');

          const params = this.sourceMode === "odata"
            ? [`$select=${select}`,`$filter=${filter}`].join('&')
            : null;

          let target = null;
          switch(this.target) {
            case TARGETS.GDrive:
              target = new GDriveExportTarget(this.entity);
              break;
            case TARGETS.SaveAs:
              target = new SaveAsTarget(this.entity);
              break;
          }


          const {promise, cancel} = exportData({
            target,
            logicalName: this.entity,
            fetchXml: this.fetchXml,
            fields,
            fieldTypes,
            params,
            exportFormattedValues: this.exportFormattedValues,
            format: this.format,
            includeEtag: this.includeEtag,
            zipResult: this.zipResult,
            pageTick: ({total, page}) => {
              this.exportCount = total;
            }
          });

          this.data.cancel = cancel;

          const {
            dataLength,
            blobSize,
            cancelled
          } = await promise;

          if(cancelled) {
            console.log(`Export cancelled`);
          }
          else {
            console.log(`Export DONE. Data length: ${dataLength}, Blob Size: ${blobSize}`);
          }
        } catch (ex) {
          console.error(ex);
          Bus.error({message: "Export failed."});
        } finally {
          this.exporting = false;
          this.data.cancel = null;
          this.cancelled = false;
        }

      },

      async onCustomFetchChanged() {
        if(this.customFetch) {
          try {
            const fetchDom = parser.parseFromString(this.customFetch, "text/xml");

            const entityAttrs = Array.from(fetchDom.querySelectorAll("entity > attribute"))
              .map(node => ({
                ...getNodeAttributes(node, "name", "alias"),
                entity: this.entity,
              }));

            const linkEntities = Array.from(fetchDom.querySelectorAll("link-entity[name]"))
              .map(node => node.getAttribute("name"));

            const [_, ...linkMetas] = await Metadata.getEntities(this.entity, ...linkEntities);

            const metas = linkEntities.reduce((m,name,i) => (m[name] = linkMetas[i], m), {
              [this.entity]: this.meta
            });

            const linkAttrs = Array.from(fetchDom.querySelectorAll("link-entity > attribute"))
              .map(node => ({
                ...getNodeAttributes(node, "name", "alias"),
                entity: node.parentElement.getAttribute("name")
              }));

            const fields = [...entityAttrs, ...linkAttrs]
              .map(({name, alias, entity}) => ({
                name, alias, entity,
                type: metas[entity].attributes[name].type,
                displayName: metas[entity].attributes[name].displayName
              }));

            this.fields = fields;

            this.customFetchValid = true;
          } catch (ex) {
            this.customFetchValid = false;
          }
        }
      },

      async onEntityChanged(newEntity) {
        this.entity = newEntity;
        this.fields = [];
        this.meta = await Metadata.getEntity(this.entity);
        const {primaryIdAttribute,primaryNameAttribute} = this.meta;

        [primaryIdAttribute,primaryNameAttribute]
          .filter(a => !!a)
          .forEach(name => {
            const {displayName, type} = this.meta.attributes[name];
            this.fields.push({
              displayName, name, type,
              entity: this.entity,
              alias: null
            });
          });
      },

      onFieldChanged(newAttr) {
        if(newAttr) {
          const {displayName, type, name} = this.meta.attributes[newAttr];
          this.fields.push({
            displayName, type, name,
            entity: this.entity,
            alias: null
          });
        }
      },

      onViewChanged(newView) {
        if(newView) {
          this.view = newView;
          const layoutDom = parser.parseFromString(this.view.layoutxml, "text/xml");

          this.fields = Array.from(layoutDom.querySelectorAll("cell"))
            .map(cell => ({
              name: cell.getAttribute("name"),
              alias: cell.getAttribute("alias")
            }))
            .filter(cell => !cell.alias && this.meta.attributes[cell.name])
            .map(cell => this.meta.attributes[cell.name]);

          const hasPK = this.fields.some(f => f.name === this.meta.primaryIdAttribute);
          if(!hasPK) {
            this.fields.unshift(this.meta.attributes[this.meta.primaryIdAttribute]);
          }
        }
      },

      removeField(field) {
        const i = this.fields.indexOf(field);
        this.fields.splice(i, 1);
      }
    }

  }
};
