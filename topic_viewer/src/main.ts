
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import "@ag-grid-community/styles/ag-grid.css"; // Mandatory CSS required by the Data Grid
import "@ag-grid-community/styles/ag-theme-quartz.css"; // Optional Theme applied to the Data Grid
import {
  type ColDef,
  type GridOptions,
  createGrid,
  type GridApi,
} from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import topicsData from './topic_data.json';

const sentimentColors = [
    "#6badc9",
    "#90c1d7",
    "#b5d6e4",
    "#daeaf2",
    "#fbe0df",
    "#f8c1c0",
    "#f8c1c0",
    "#f4a3a0",
    "#f18e81",
    "#ed6f61"
];

ModuleRegistry.registerModules([
  ClientSideRowModelModule
]);

let gridApi!: GridApi;

const columnDefs: ColDef[] = [
    {
        field: "topic",
    //headerName: "Album Name",
    //cellRenderer: "agGroupCellRenderer",
    //headerClass: "header-product",
    //cellRendererParams: {
    //  innerRenderer: ProductCellRenderer,
    //},
    //minWidth: 300,
        width: 350,
        resizable: true
    },
    {
        field: "posts",
        headerName: "Posts",
    },
    {
        field: "sentiment",
        headerName: "Sentiment",
    },
    {
        field: "divisive",
        headerName: "Divisive?",
    }
];

function formatData() {
    let data = [];
    for (let key in topicsData){
        const e = topicsData[key];
        data.push({
            topic: key,
            posts: e.num_posts,
            sentiment: e.median_score,
            divisive: e.divisiveness.toFixed(2)
        });
    }
    return data;
}

function renderTable(posts) {
    let html = "";
    for (let i in posts){
        const p = posts[i];
        const row = "<tr><td class='score'>" + p.score + "</td>" +
                    "<td class='link'><a target='_blank' href='" + p.url + "'>" +
                    p.title + "</a></td></tr>";
        html += row;
    }
    console.log('hey', html);
    return html;
}

function selectRow(topic) {
    const e = topicsData[topic];
    document.querySelector("#topictitle").innerHTML = topic;
    const angryT = document.querySelector("#angry_table") as HTMLElement;
    angryT.innerHTML = renderTable(e.angry_posts);
    const happyT = document.querySelector("#happy_table") as HTMLElement;
    happyT.innerHTML = renderTable(e.happy_posts);
}

const rowData = formatData();

const defaultColDef = {
  resizable: false,
};

const gridOptions: GridOptions = {
  columnDefs,
  rowData,
  defaultColDef,
  getRowStyle: p => { return { background: sentimentColors[9 - p.data.sentiment] }},
  onCellMouseOver: (p) => { selectRow(p.data.topic) },
  //rowHeight: 80,
  //paginationPageSizeSelector: [5, 10, 20],
  //pagination: true,
  rowModelType: 'clientSide',
  rowSelection: 'single',
  //paginationPageSize: 10,
 // masterDetail: true,
  //detailRowAutoHeight: true,
  suppressCellFocus: true,
  autoSizeStrategy: {
    type: "fitGridWidth",
  },
  //detailCellRendererParams,
};

document.addEventListener("DOMContentLoaded", function () {
    console.log('ds');
    const gridDiv = document.querySelector("#app") as HTMLElement;
    gridApi = createGrid(gridDiv, gridOptions);
    const filterTextBox = document.getElementById("filter-text-box");
    filterTextBox!.addEventListener("input", (event) => {
        // @ts-ignore
        const value = event.target?.value;
        gridApi.setGridOption("quickFilterText", value);
    });
});
