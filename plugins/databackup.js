import Bus from '../../bus.js';
import {DropZone, fileDialog, pageAll, readAsText} from '../../lib/helpers.js';

import DialogComponent from '../../components/dialog.js';
import {FilterableEntitySelector as EntitySelector} from '../../components/entityselector.js';
import {RibbonBar, RibbonButton, RibbonFlyout} from '../../components/form.js';
import ProgressBar from '../../components/progress.js';

import {GetAttributes} from '../../lib/metadatacache.js';

const exportableAttribute = ({IsValidForCreate, IsValidForRead}) => IsValidForCreate && IsValidForRead;

// Some commonly used aliases
const {Service,Metadata} = ORBIS.Core;

const ID = 'JSToolbox.ORBIS.DataBackup';
const PLUGINNAME = 'Configuration Data Backup';
const PLUGINICON = 'download'; // any of the FontAwesome icons (https://fontawesome.com/v4.7.0/icons/)
const PLUGINDESCRIPTION = 'Backup Configuration Data';
const PLUGINCSSCLASS = 'orb-jstoolbox-databackup'; // e.g. "orb-plugin-sonepar-dosomestuff"
const PLUGINVERSION = '0.9.0';
const PLUGINAUTHOR = 'Daniel Bruckhaus';
const PLUGINEMAIL = 'daniel.bruckhaus@orbis.de';

const SCHEMAS_KEY = `${ID}.ExportSchemas`;

const ONE_HOUR = 1000 * 60 * 60;

const Defaults = {
  get Entity () {
    return {
      LogicalName: null,
      Fields: [],
      FetchXml: null,
      Mode: 'fetch'
    };
  },

  get Schema () {
    return {
      Name: null,
      Entities: [],
      ExportedOn: null,
      Interval: null
    };
  }
};

Bus.addStyle(`
  .${PLUGINCSSCLASS} .dropzone.active {
    border-color: #777;
  }
`, ID);

export default {
  id: ID,
  version: PLUGINVERSION,
  author: PLUGINAUTHOR,
  email: PLUGINEMAIL,
  name: PLUGINNAME,
  icon: PLUGINICON,
  //stylesheet: './plugins/jsoneditor.css', // Optional: A stylesheet path, relative to index.html
  component: {
    components: {
      DialogComponent,
      DropZone,
      EntitySelector,
      ProgressBar,
      RibbonBar, RibbonButton, RibbonFlyout
    },
    filters: {
      time(value, fallback = '--') { return value ? new Date(value).toLocaleString() : fallback; }
    },
    template: `
      <div class="${PLUGINCSSCLASS} flex flex-col">
        <ribbon-bar>
          <ribbon-button @execute="newSchema()" faicon="plus" title="New Schema">New Schema</ribbon-button>
          <ribbon-button @execute="saveSchemas()" faicon="floppy-o" title="Save Export Schemas">Save Export Schemas</ribbon-button>
          <ribbon-flyout faicon="cog">
            Schema Options
            <template v-slot:flyout>
              <ribbon-button @execute="exportSchemas()" faicon="download" title="Export">Export Schema Definitions</ribbon-button>
              <ribbon-button @execute="importSchemas()" faicon="upload" title="Import">Import Schema Definitions</ribbon-button>
            </template>
          </ribbon-flyout>

        </ribbon-bar>
        <table class="data-table">
          <tr>
            <th>Name</th>
            <th>No. of Entities</th>
            <th>Last exported on</th>
            <th>Export Interval (h)</th>
            <th>Next export due</th>
          </tr>
          <tr class="data-row cursor-pointer" v-for="s in schemas" @click="schema = s, entity = null">
            <td>{{s.Name}}</td>
            <td>{{s.Entities.length}}</td>
            <td>{{s.ExportedOn | time('never') }}</td>
            <td>{{s.Interval}}</td>
            <td>{{nextExport(s) | time}}</td>
          </tr>
        </table>
        <progress-bar v-if="currentExport.running" :min="0" :max="currentExport.max" :value="currentExport.index" :text="currentExport.entity"></progress-bar>
        <section>
          <drop-zone class="border-dashed border-grey-light dropzone my-4 px-2 h-48" @files-dropped="onFilesDropped"><p>Drag one or more JSON files here to restore backups ...</p></drop-zone>
        </section>
        <section v-if="schema" class="my-2">
          <h1>{{schema.Name}}</h1>
          <ribbon-bar>
            <ribbon-button faicon="download" @execute="exportData(schema)">Export Now</ribbon-button>
            <ribbon-button faicon="plus" @execute="newEntity()">Add Entity</ribbon-button>
            <ribbon-button faicon="trash" @execute="remove(schemas, schema), schema = null">Remove Schema</ribbon-button>
          </ribbon-bar>
          <div class="flex flex-row items-center">
            <label class="contents">
              <span class="p-2">Name</span>
              <input type="text" v-model="schema.Name" class="flex-1"/>
            </label>
            <label class="contents">
              <span class="p-2" title="The interval (in hours) at which to export this schema automatically">Interval</span>
              <input type="number" v-model.number="schema.Interval" class="flex-1"/>
            </label>
          </div>

          <table class="data-table my-2">
            <tr>
              <th>Entity</th>
              <th class="text-left">Fields/FetchXml</th>
            </tr>
            <tr class="data-row cursor-pointer" v-for="e in schema.Entities" @click="entity = e">
              <td>{{e.LogicalName}}</td>
              <td>
                <span v-if="e.FetchXml" :title="e.FetchXml">{{e.FetchXml}}</span>
                <span v-if="e.Fields && e.Fields.length">{{e.Fields.length}} Fields: {{e.Fields.join(', ')}}</span>
              </td>
            </tr>
          </table>

        </section>
        <dialog-component v-if="entity" :title="entityDialogTitle" :esc-cancels="true" @dialog-close="entity = null">
          <ribbon-bar>
            <ribbon-button faicon="floppy-o" @execute="addEntity(schema, entity)" v-if="!schema.Entities.includes(entity)" :can-execute="canAddEntity">Add to Schema</ribbon-button>
            <ribbon-button faicon="magic" @execute="autoAddFields(entity)" :can-execute="canAutoAddFields">Auto-Add Fields</ribbon-button>
            <ribbon-button v-if="includes(schema.Entities, entity)" faicon="trash" @execute="remove(schema.Entities, entity), entity = null">Remove Entity</ribbon-button>
          </ribbon-bar>
          <label class="py-2">
            <span>Mode</span>
            <select v-model="entity.Mode">
              <option value="fetch">Export by FetchXml</option>
              <option value="fields">Export all records, and specific fields</option>
            </select>
          </label>
          <template v-if="'fetch' === entity.Mode">
            <p>Entity: {{entity.LogicalName || '--'}}</p>
            <textarea class="w-full" rows="10" v-model="entity.FetchXml" @change="onEntityFetchChanged()" placeholder="Enter/Paste FetchXml here..."></textarea>
          </template>
          <template v-if="'fields' === entity.Mode">
            <entity-selector :logicalname="entity.LogicalName" @selection-changed="entity.LogicalName = $event"></entity-selector>
            <p>{{entity.Fields.length}} Fields:</p>
            <ul>
              <li v-for="(f,i) in entity.Fields" class="px-2 hover:bg-red-lighter hover:line-through" title="Click to remove" @click="removeAt(entity.Fields, i)" >{{f}}</li>
            </ul>
          </template>
        </dialog-component>
      </div>
    `,
    data () {
      return {
        currentExport: {
          max: 0,
          entity: null,
          index: 0,
          running: false,
        },
        entity: null,
        interval: null,
        schema: null,
        schemas: [],
      };
    },

    // Executed once the plugin component is loaded
    created () {
      this.loadSchemas();
      this.interval = setInterval(this.autoExport, 60 * 1000);
    },

    // Executed once the plugin component has been added to the DOM
    mounted  () {

    },

    beforeDestroy() {
      clearInterval(this.interval);
    },

    // Computed properties
    computed: {
      canAddEntity() {
        const {LogicalName, FetchXml, Fields} = this.entity;
        return LogicalName && (!!FetchXml || Fields.length > 0);
      },

      canAutoAddFields() { return !!this.entity.LogicalName && this.entity.Mode === 'fields'; },

      entityDialogTitle() {
        if(this.schema.Entities.includes(this.entity)) {
          return this.entity.LogicalName;
        }
        else {
          return 'Add a new entity to this schema';
        }
      }
    },

    // Methods, Eventhandlers, etc.
    methods: {
      async addEntity(schema, entity) {
        schema.Entities.push(entity);
      },

      async autoAddFields() {
        try {
          const Fields = (await GetAttributes(this.entity.LogicalName))
            .filter(exportableAttribute)
            .map(a => a.LogicalName);

          Fields.sort();

          this.entity.Fields = Fields;
        } catch (e) {
          console.error(e);
          Bus.error({message: "Auto-Add Fields failed"});
        }
      },

      async autoExport() {
        const now = new Date().getTime();



        console.log(`Checking for due exports at ${now}`);

        const due = this.schemas
          .filter(s => s.Interval)
          .filter(s => !s.ExportedOn || s.ExportedOn + (s.Interval * ONE_HOUR) < now);

        if(due.length) {
          Bus.info({message: `${due.length} Data Export Schemas are due for export. Exporting now...`});
          for(const schema of due) {
            try {
              await this.exportData(schema);
            } catch (e) {
              console.error(e);
              Bus.error(`Export Failed for Schema ${schema.Name}`);
            }
          }

          this.saveSchemas();
        }
        else {
          console.log(`No data exports due`);
        }
      },

      async exportData(schema) {
        try {
          this.currentExport.running = true;
          this.currentExport.max = schema.Entities.length - 1;

          const Results = [];

          let index = 0;
          for(const {LogicalName, Fields = [], FetchXml = null} of schema.Entities) {
            Object.assign(this.currentExport, {index, entity: LogicalName});
            index++;

            if(![Fields.length, FetchXml].some(x => !!x)) {
              console.warn(`Neither Fields nor FetchXml specified for ${LogicalName} - skipping`);
              continue;
            }

            let fxml = null;
            if(Fields.length) {
              const attrs = Fields.map(ln => `<attribute name="${ln}" />`).join('');

              fxml = `<fetch distinct="false" no-lock="true"><entity name="${LogicalName}">${attrs}</entity></fetch>`;
            }
            else if (FetchXml) {
              fxml = FetchXml;
            }

            const Records = await this.getAll(fxml);
            console.log(` ${Records.length} records`);
            Results.push({LogicalName, Records});
          }

          const timestamp = new moment().format("YYYY-MM-DDTHH-mm-ss");
          download(JSON.stringify(Results, null, 2), `${schema.Name}.${timestamp}.json`, 'application/json');

          schema.ExportedOn = new Date().getTime();
        } catch (e) {
          console.error(e);
          Bus.error({message: "Export failed"});
        } finally {
          this.currentExport.running = false;
        }

      },

      exportSchemas() {
        const json = JSON.stringify(this.schemas, null, 2);
        download(json, `Configuration Data Backup Schema Definitions.json`, "application/json");
      },

      async importSchemas() {
        debugger;
        const jsonFile = await fileDialog({
          message: "Select Configuration Data Backup Schema Definitions JSON file"
        });

        const json = await readAsText(jsonFile);

        try {
          const schemas = JSON.parse(json);

          debugger;
        } catch (e) {
          console.error(e);
          Bus.error({message: "Schema import failed."});
        }
      },

      includes(array, item) {
        return array.includes(item);
      },

      async getAll(fxml) {
        const all = [];
        for await (const page of pageAll(fxml)) {
          all.push(...page);
        }
        return all;
      },

      loadSchemas() {
        const json = localStorage.getItem(SCHEMAS_KEY);
        if(json) {
          const schemas = JSON.parse(json);
          this.schemas = schemas.map(s => Object.assign({}, Defaults.Schema, s));
        }
      },

      newEntity(schema) {
        this.entity = Defaults.Entity;;
      },

      newSchema() {
        const schema = Object.assign({}, Defaults.Schema);
        this.schemas.push(schema);
        this.schema = schema;
      },

      nextExport(schema) {
        const {Interval, ExportedOn = 0} = schema;

        if(Interval) {
          return ExportedOn + Interval * ONE_HOUR;
        }
        else {
          return null;
        }
      },

      onEntityFetchChanged() {
        let LogicalName = null;
        if(this.entity.FetchXml) {
          const lnMatch = this.entity.FetchXml.match(/<entity[^>*]name="(\w+)">/);
          if(lnMatch) {
            LogicalName = lnMatch[1];
          }
        }
        this.entity.LogicalName = LogicalName;
      },

      async onFilesDropped(files) {
        const importQueue = [];

        for(const file of files) {
          const {name} = file;
          try {
            const json = await readAsText(file);
            const data = JSON.parse(json);
            importQueue.push(...data);
          }
          catch(ex) {
            Bus.error(`Failed to process dropped file ${name}`);
          }
        }

        debugger;
      },

      remove(array, item) {
        const i = array.indexOf(item);
        if(i > -1) {
          array.splice(i, 1);
        }
      },

      removeAt(array, index) {
        array.splice(index, 1);
      },

      saveSchemas() {
        const json = JSON.stringify(this.schemas);
        localStorage.setItem(SCHEMAS_KEY, json);
      }
    }

  }
};
