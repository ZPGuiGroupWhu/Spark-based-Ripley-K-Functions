import React from 'react';

//导入折线图
import 'echarts/lib/chart/pie';  //折线图是line,饼图改为pie,柱形图改为bar
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/markPoint';
import ReactEcharts from 'echarts-for-react';

export default class DataIntro extends React.Component {

  getOption =()=> {
    let option = {
      tooltip: {
        trigger: 'item',
        formatter: '{b} <br/> {c} ({d}%)'
      },
      series: [
        {
          name: '企业数据',
          type: 'pie',
          radius: '55%',
          center: ['50%', '45%'],
          labelLine: {
            lineStyle: {
              color: '#ffffff'
            }
          },
          data: [
            {
              value: 33524,
              name: '第一产业',
              label: {
                color: '#ffffff',
              },
              itemStyle: {
                color: {
                  type: 'radial',
                  x: -0.1,
                  y: 0.6,
                  r: 1,
                  colorStops: [{
                    offset: 0, color: 'rgba(255,70,70,1)' // 0% 处的颜色
                  }, {
                    offset: 1, color: 'rgba(255,70,70,0.3)' // 100% 处的颜色
                  }],
                  global: false // 缺省为 false
                }
              }
            },
            {
              value: 31023,
              name: '第二产业',
              label: {
                color: '#ffffff',
              },
              itemStyle: {
                color: {
                  type: 'radial',
                  x: 0.6,
                  y: 0,
                  r: 1,
                  colorStops: [{
                    offset: 0, color: 'rgba(70,255,70,1)' // 0% 处的颜色
                  }, {
                    offset: 1, color: 'rgba(70,255,70,0.3)' // 100% 处的颜色
                  }],
                  global: false // 缺省为 false
                }
              }
            },
            { value: 23544, 
              name: '第三产业', 
              label: {
                color: '#ffffff',
              },
              itemStyle: {
                color: {
                  type: 'radial',
                  x: 1,
                  y: 1,
                  r: 1,
                  colorStops: [{
                    offset: 0, color: 'rgba(70,70,255,1)' // 0% 处的颜色
                  }, {
                    offset: 1, color: 'rgba(70,70,255,0.3)' // 100% 处的颜色
                  }],
                  global: false // 缺省为 false
                }
              }
            },
          ],
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0, 0.5)'
            }
          }
        }
      ]
    }
   return option
  }

  render() {
    return <div>
        <h3>数据概况</h3>
        <ReactEcharts option={this.getOption()} theme="Imooc"  style={{height:'200px'}}/>
      </div>
  }
}
