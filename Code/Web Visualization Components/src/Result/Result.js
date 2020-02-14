import React from 'react';

//导入折线图
import 'echarts/lib/chart/line';  //折线图是line,饼图改为pie,柱形图改为bar
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/markPoint';
import ReactEcharts from 'echarts-for-react';
import data from './confidence.json';

const base = -data.reduce(function (min, val) {
  return Math.floor(Math.min(min, val.l));
}, Infinity);
let i =-160;

export default class Result extends React.Component {

  getOption = () => {
    let option = {
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          animation: false,
          label: {
            backgroundColor: '#ccc',
            borderColor: '#aaa',
            borderWidth: 1,
            shadowBlur: 0,
            shadowOffsetX: 0,
            shadowOffsetY: 0,

            color: '#222'
          }
        },
        formatter: function (params) {
          return params[2].value;
        }
      },
      grid: {
        top: '3%',
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: data.map(function (item) {
          return item.date;
        }),
        axisLabel: {
          formatter: function (value, idx) {
            var date = new Date(value);
            // return idx === 0 ? value : [date.getMonth() + 1, date.getDate()].join('-');
            return i++;
          }
        },
        splitLine: {
          show: false
        },
        boundaryGap: false,
        axisLine:{
          lineStyle:{
              color:'#ffffff',
          }
        }, 

      },
      yAxis: {
        axisLabel: {
          formatter: function (val) {
            return (val - base);
          }
        },
        axisPointer: {
          label: {
            formatter: function (params) {
              return ((params.value - base) * 100).toFixed(1) + '%';
            }
          }
        },
        splitNumber: 3,
        splitLine: {
          show: false
        },
        axisLine:{
          lineStyle:{
              color:'#ffffff',
          }
        },
      },
      series: [{
        name: 'L',
        type: 'line',
        data: data.map(function (item) {
          return item.l + base;
        }),
        lineStyle: {
          opacity: 0
        },
        stack: 'confidence-band',
        symbol: 'none'
      }, {
        name: 'U',
        type: 'line',
        data: data.map(function (item) {
          return item.u - item.l;
        }),
        lineStyle: {
          opacity: 0
        },
        areaStyle: {
          color: '#ccc'
        },
        stack: 'confidence-band',
        symbol: 'none'
      }, {
        type: 'line',
        data: data.map(function (item) {
          return item.value + base;
        }),
        hoverAnimation: false,
        symbolSize: 6,
        itemStyle: {
          color: '#c23531'
        },
        showSymbol: false
      }],
    };
    return option;
  }

  render() {
    return <div>
        <h3>结果展示</h3>
        <ReactEcharts option={this.getOption()} theme="Imooc"  style={{height:'200px'}}/>
      </div>
  }
}
