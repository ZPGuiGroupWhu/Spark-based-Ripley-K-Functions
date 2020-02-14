import React from 'react';
import './CalcuInfo.css';
import { Button, Progress } from 'antd';

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