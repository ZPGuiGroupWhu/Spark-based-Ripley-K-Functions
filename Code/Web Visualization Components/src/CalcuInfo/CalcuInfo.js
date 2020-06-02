import React from 'react';
import './CalcuInfo.css';
import { Button, Progress } from 'antd';
import { getURLWithParam } from '../common/tool';
// import calResult from '../common/result.json';

const CALCUSTATE = {
  BEFRORECALCU: 0,
  PARAMERROR: -1,
  CALCUING: 1,
  FINISHED: 2,
  SERVERERROR: -2,
};

export default class CalcuInfo extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      calcuState: CALCUSTATE.BEFRORECALCU,
      percent:0,
    };
  };

  startCalcu = () => {
    
    const params = this.props.params;
    const {KType, DataCate, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime} = params;
    console.log(this.props.params);
    if(KType === 'Cross'){
      if(params.DataCate[0] === params.DataCate[1]){
        alert('交叉K函数入参点数据类型不能相同');
        return false;
      }
    }
    const commitParam = {
      dataSize: 50000,
      maxSpatialDistance: 20,
      maxTemporalDistance: 20,
      spatialStep: 20,
      temporalStep: 20,
      numExecutors: 8,
      executorCores: 8,
      executorMemory: '14g',
    };
    const kType = {
      type: this.props.params.KType.toLowerCase(),
    }
    const url = 'http://localhost:8011/k-result';

    const urlParam = getURLWithParam(url, kType);
    console.log('request url', urlParam);
    // 能正确请求到结果
    fetch(urlParam)
   .then((response) => response.json())
   .then((responseJson) => {
     responseJson.time = new Date();
    this.props.getCalResult(responseJson);
    console.log(responseJson.maxSpatialDistance);
   })
   .catch((error) => {
    console.error('请求计算结果出错', error);
   });

    this.setState({calcuState: CALCUSTATE.CALCUING, percent: 0}, this.changePercent);
  }

  changePercent = () => {
    if(this.state.percent < 100) {
      setTimeout(()=>{
        this.setState({percent: this.state.percent + 1})
        this.changePercent();
      }, 50);
    } else{
      this.setState({calcuState: CALCUSTATE.FINISHED});
      // 先使用示例数据
      // this.props.getCalResult(calResult);
    }
  }

  render() {
    return <div className="calcuInfo">
        <h3>集群计算信息</h3>
        { 
          this.state.calcuState === CALCUSTATE.BEFRORECALCU &&
            <Button onClick={this.startCalcu}>点击开始计算</Button>
        }
        { 
          (this.state.calcuState === CALCUSTATE.CALCUING || this.state.calcuState === CALCUSTATE.FINISHED)&&
          <div className="cal-progress">
            <Progress percent={this.state.percent}/>
            <br/> 计算中...<br/>
            <Button>查看详情</Button> {this.state.calcuState === CALCUSTATE.FINISHED && <Button onClick={this.startCalcu}>重新计算</Button>}
          </div>
        }
        
      </div>
  }
}