import React from 'react';
import './App.css';
import DataIntro from './DataIntro/DataIntro';
import VisualController from './VisualController/VisualController';
import ModuleContainer from './ModuleContainer/ModuleContainer';
import Parameter from './Parameter/Parameter';
import DataSource from './DataSource/DataSource';
import DataRange from './DataRange/DataRange';
import CalcuInfo from './CalcuInfo/CalcuInfo';
import DateStatistic from './DateStatistic/DateStatistic';
import Result from './Result/Result';
import Map from './Map/Map';
import Layer from './Layer/Layer';
import Nano from './Nano/Nano';


export default class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      dimension: 3,
      colorObj: {
        isRShow: true, 
        isGShow: true,
        isBShow: true,
        RContent: 1,
        GContent: 2,
        BContent: 3,
      },
      layer: 2,
    };
  };
  changeDimension = (isChecked) => {
    this.setState({dimension : isChecked ? 3 : 2});
  };

  changeColor = (colorObject) => {
    this.setState({colorObj: colorObject});
  };

  changeLayer = (layer) => {
    this.setState({layer: layer});
  };

  render() {
    const layer = this.state.layer;
    return (
      <div>
        <div className={layer === 2 ? 'hidden' : ''}>
          <Map dimension={this.state.dimension} colorObj={this.state.colorObj} isRShow={this.state.isRShow} />
        </div>
        <div className={layer === 1 ? 'hidden' : ''}>
          <Nano />
        </div>
        <div className={`left-moudles ${layer === 2 ? 'bottom' : ''}`}>
          <ModuleContainer  title="点数据概况" close="true">
            <DataIntro />
          </ModuleContainer>
          <ModuleContainer  title="点数据展示控制" hidden={layer === 2}>
            <VisualController changeDimension={this.changeDimension} changeColor={this.changeColor}/>
          </ModuleContainer>
          <ModuleContainer  title="图层选择" >
            <Layer changeLayer={this.changeLayer}/>
          </ModuleContainer>
           <ModuleContainer  title="时间统计" autowidth="true" dark="true" hidden={layer === 1}>
            <DateStatistic />
          </ModuleContainer>
        </div>
        <div className="right-moudles">
          <ModuleContainer  right="true" title="数据源" close="true">
            <DataSource />
          </ModuleContainer>
          <ModuleContainer  right="true" title="研究范围" close="true">
            <DataRange />
          </ModuleContainer>
          <ModuleContainer  right="true" title="参数选择" >
            <Parameter />
          </ModuleContainer>
          <ModuleContainer  right="true" title="计算信息" >
            <CalcuInfo />
          </ModuleContainer>
          <ModuleContainer  right="true" title="结果展示" >
            <Result />
          </ModuleContainer>
        </div>
      </div>
    );
  }
}
