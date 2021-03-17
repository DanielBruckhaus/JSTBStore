import Bus from '../../bus.js';

import {getWebAPIUrl} from '../../lib/helpers.js';
const WEBAPIURL = getWebAPIUrl();

import {RibbonBar, RibbonButton} from '../../components/form.js';
import {FilterableEntitySelector} from '../../components/entityselector.js';
import AttributeSelector from '../../components/attributeselector.js';

// Some commonly used aliases
const {Service,Metadata} = ORBIS.Core;

const ID = 'ORBIS.JSToolbox.PicklistBrowser'; // e.g. "Orbis.JSToolbox.Plugins.Unmanaged.Sonepar.DoSomeStuff"
const PLUGINNAME = 'Picklist Browser'; // e.g. "SPR: Do some Stuff"
const PLUGINICON = 'file'; // any of the FontAwesome icons (https://fontawesome.com/v4.7.0/icons/)
const PLUGINDESCRIPTION = null;
const PLUGINKEYWORDS = ["picklist", "optionset", "browser", "search"];
const PLUGINCSSCLASS = null; // e.g. "orb-plugin-sonepar-dosomestuff"
const PLUGINVERSION = '1.0.0';
const PLUGINAUTHOR = 'Daniel Bruckhaus';
const PLUGINEMAIL = 'daniel.bruckhaus@orbis.de';

const baseLcid = ORBIS.Core.Context.Xrm.getOrgLcid();

const getLanguages = async () => {
  const [availableResp, allResp] = await Promise.all([
    Service.execute(new Service.Requests.Request({
      webAPI: {
        url: "RetrieveAvailableLanguages()",
        valid: true
      }
    })),
    Service.retrieveMultiple("languagelocale", "$select=language,localeid")
  ]);

  const byLcid = allResp.data.values.reduce((m, {language:name,localeid:lcid}) => (m[lcid] = name, m), {});

  return availableResp.data.LocaleIds.map(lcid => ({
    lcid,
    name: byLcid[lcid]
  }));
};

//https://stackoverflow.com/a/35970186/10738090
function invertColor(hex, bw) {
    if (hex.indexOf('#') === 0) {
        hex = hex.slice(1);
    }
    // convert 3-digit hex to 6-digits.
    if (hex.length === 3) {
        hex = hex[0] + hex[0] + hex[1] + hex[1] + hex[2] + hex[2];
    }
    if (hex.length !== 6) {
        throw new Error('Invalid HEX color.');
    }
    var r = parseInt(hex.slice(0, 2), 16),
        g = parseInt(hex.slice(2, 4), 16),
        b = parseInt(hex.slice(4, 6), 16);
    if (bw) {
        // http://stackoverflow.com/a/3943023/112731
        return (r * 0.299 + g * 0.587 + b * 0.114) > 186
            ? '#000000'
            : '#FFFFFF';
    }
    // invert color components
    r = (255 - r).toString(16);
    g = (255 - g).toString(16);
    b = (255 - b).toString(16);
    // pad each with zeros and return
    return "#" + padZero(r) + padZero(g) + padZero(b);
}
function padZero(str, len) {
    len = len || 2;
    var zeros = new Array(len).join('0');
    return (zeros + str).slice(-len);
}

const PicklistAttributeTypes = [
  "Picklist",
  "MultiSelectPicklist",
  "State",
  "Status",
  "EntityName",
  "Boolean"
];

export default {
  id: ID,
  version: PLUGINVERSION,
  author: PLUGINAUTHOR,
  email: PLUGINEMAIL,
  name: PLUGINNAME,
  description: PLUGINDESCRIPTION,
  keywords: PLUGINKEYWORDS,
  icon: PLUGINICON,
  //stylesheet: './plugins/jsoneditor.css', // Optional: A stylesheet path, relative to index.html
  component: {
    components: {
      AttributeSelector,
      EntitySelector: FilterableEntitySelector,
      RibbonBar, RibbonButton,
    },
    template: `
      <div class="${PLUGINCSSCLASS} overflow-auto">
        <div class="label-input-grid">
          <label>
            <span>Entity</span>
            <EntitySelector :logicalname="entity" @selection-changed="OnEntityChanged"></EntitySelector>
          </label>
          <label>
            <span>Attribute</span>
            <AttributeSelector v-if="entity" :entityname="entity" :logicalname="attribute" :show-type="true" :filter="attributeFilter" @selection-changed="OnAttributeChanged"></AttributeSelector>
            <span v-else class="italic">Select an Entity</span>
          </label>
          <template v-if="optionset">
            <div>
              <span>Name</span>
              <span>{{optionset.name}}</span>
            </div>
            <div>
              <span>Global</span>
              <span>{{optionset.isglobal}}</span>
            </div>
            <div>
              <span title="Attribute default value">Default</span>
              <span>{{optionset.defaultValue}}</span>
            </div>
            <div>
              <span># Options</span>
              <span>{{optionset.options.length}}</span>
            </div>
          </template>
        </div>
        <table  v-if="optionset" class="data-table overflow-auto">
          <tr class="header-row">
            <th>Value</th>
            <th v-if="'Status' === optionset.type">State</th>
            <th v-if="'State' === optionset.type">Default Status</th>
            <th v-for="l in languages">
              <span>{{l.name}} ({{l.lcid}})</span>
              <span v-if="l.lcid === baseLcid">[Base]</span>
            </th>
            <th>Color</th>
          </tr>
          <tr class="data-row" v-for="option in optionset.options">
            <td>
              <span>{{option.Value}}</span>
              <span v-if="option.Value === optionset.defaultValue">(Default)</span>
            </td>
            <td v-if="'Status' === optionset.type">{{option.State}}</td>
            <td v-if="'State' === optionset.type">{{option.DefaultStatus}}</td>
            <td v-for="l in languages">{{option.Labels[l.lcid]}}</td>
            <td :style="{'background-color': option.Color, 'color': option.TextColor}">{{option.Color}}</td>
          </tr>
        </table>
      </div>
    `,
    data () {
      return {
        baseLcid,
        attribute: null,
        entity: "account",
        languages: [],
        optionset: null
      };
    },

    // Executed once the plugin component is loaded
    async created () {
      this.languages = await getLanguages();
    },

    // Executed once the plugin component has been added to the DOM
    mounted  () {

    },

    // Computed properties
    computed: {

    },

    // Methods, Eventhandlers, etc.
    methods: {
      attributeFilter(attr) {
        return PicklistAttributeTypes.includes(attr.type);
      },
      async loadPicklist() {
        this.optionset = null;
        if(this.entity && this.attribute) {
          const {entity, attribute} = this;
          const baseUrl = `${WEBAPIURL}/EntityDefinitions(LogicalName='${entity}')/Attributes(LogicalName='${attribute}')`;
          const baseResp = await fetch(baseUrl);
          const baseData = await baseResp.json();
          const type = baseData["@odata.type"].substr(1);
          const url = `${WEBAPIURL}/EntityDefinitions(LogicalName='${entity}')/Attributes(LogicalName='${attribute}')/${type}?$expand=OptionSet`;
          const resp = await fetch(url);
          const data = await resp.json();

          const {DefaultValue, DefaultFormValue} = data;
          const {OptionSetType, Options, TrueOption, FalseOption, Name:name, IsGlobal:isglobal} = data.OptionSet;
          let options;
          let defaultValue = null;

          switch (OptionSetType) {
            case "Boolean":
              options = [TrueOption, FalseOption];
              defaultValue = DefaultValue ? 1 : 0;
              break;
            default:
              options = Options;
              defaultValue = DefaultFormValue > -1 ? DefaultFormValue : null;
          }

          this.optionset = {
            name, isglobal, defaultValue,
            type: OptionSetType,
            options: options.map(({Value,Color,Label,DefaultStatus,State}) => ({
              Value, Color, DefaultStatus, State,
              TextColor: Color ? invertColor(Color, true) : null,
              Labels: Label.LocalizedLabels.reduce((m,{Label,LanguageCode}) => (m[LanguageCode] = Label, m), {})
            }))
          };
        }
      },

      OnAttributeChanged(newAttribute) {
        this.attribute = newAttribute;
        this.loadPicklist();
      },

      OnEntityChanged(newEntity) {
        this.entity = newEntity;
        this.attribute = null;
        this.optionset = null;
      }
    }

  }
};
