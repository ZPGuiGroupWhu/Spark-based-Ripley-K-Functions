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
import NanoCharts from './NanoCharts/NanoCharts';
import intl from 'react-intl-universal';

const locales = {
  "en": require('./locales/en-US.json'),
  "zh": require('./locales/zh-CN.json'),
};

export default class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      dimension: 3,
      scale: 14,
      colorObj: {
        isRShow: true,
        isGShow: true,
        isBShow: true,
        RContent: 1,
        GContent: 2,
        BContent: 3,
      },
      layer: 1,
      params: {
        KType: 'ST',
        DataName:'hubei.csv',
        DataName_2:'',
        DataCate1: 0,
        DataCate2: 1,
        SpatialMax: 2000,
        TimeMax: 20,
        SpatialStep: 20,
        TimeStep: 20,
        simuTime: 100,
      },
      calResult: null,
      ChartData: null,
      DataMap:null,
      DataMap_2:null,
      initDone: false,
      langButton: localStorage.defaultLng === 'zh' ? 'English' : '中文'
    };
  };
  componentDidMount() {
    let lang = (navigator.language || navigator.browserLanguage).toLowerCase();
    if (lang.indexOf('zh') >= 0) {
      // 假如浏览器语言是中文
      localStorage.setItem("defaultLng", "zh")
    } else {
      // 假如浏览器语言是其它语言
      localStorage.setItem("defaultLng", "en")
    }
    this.loadLocales();
  }

  loadLocales() {
    intl.init({
      currentLocale: localStorage.getItem('locale') || localStorage.getItem("defaultLng") || 'zh',
      locales,
    })
      .then(() => {
        this.setState({ initDone: true });
      });
  }

  changeDimension = (isChecked) => {
    this.setState({ dimension: isChecked ? 3 : 2 });
  };

  changeColor = (colorObject) => {
    this.setState({ colorObj: colorObject });
  };

  changeScale = (scale) => {
    this.setState({ scale: scale });
  }

  changePieChart = (ChartData) => {
    this.setState({ChartData:ChartData});
    // console.log(this.state.ChartData)
  }

  changeDataMap = (DataMap) => {
    this.setState({DataMap:DataMap});
    // console.log(this.state.DataMap)
  }

  changeDataMap_2 = (DataMap) => {
    this.setState({DataMap_2:DataMap});
    // console.log(this.state.DataMap)
  }

  changeLayer = (layer) => {
    this.setState({ layer: layer });
  };

  updateParams = (params) => {
    params['DataName']=this.state.params.DataName;
    this.setState({ params: params });
    // console.log("App -> params", this.state.params)
  }

  updateDataName = (DataName) => {
    let params=this.state.params;
    params.DataName=DataName;
    this.setState({ params: params });
    // console.log("App -> params", this.state.params)
  }

  updateDataName_2 = (DataName) => {
    let params=this.state.params;
    params.DataName_2=DataName;
    this.setState({ params: params });
    // console.log("App -> params", this.state.params)
  }

  getCalResult = (calResult) => {
    this.setState({ calResult: calResult });
  }

  changeLanguage = () => {
    let lang = intl.options.currentLocale;
    if (lang === 'en') {
      this.setState({ langButton: 'English' })
      intl.options.currentLocale = 'zh'
    } else {
      this.setState({ langButton: '中文' })
      intl.options.currentLocale = 'en'
    }
  }

  render() {
    const layer = this.state.layer;
    return (
      this.state.initDone && <div>
        <div className={layer === 2 ? 'hidden' : ''}>
          <Map dimension={this.state.dimension} colorObj={this.state.colorObj} scale={this.state.scale} changePieChart={this.changePieChart} DataMap={this.state.DataMap}/>
        </div>
        <div className={layer === 1 ? 'hidden_2' : ''}>
          <Nano />
        </div>
        <div className={`left-moudles ${layer === 2 ? 'bottom' : ''}`}>
          <div className="language-change" onClick={this.changeLanguage}>{this.state.langButton}</div>
          <ModuleContainer title={intl.get('POI_OVERVIEW')} close="true" hidden={layer === 2}>
            <DataIntro ChartData={this.state.ChartData} />
          </ModuleContainer>
          <ModuleContainer title={intl.get('CONTROL_PANEL')} hidden={layer === 2}>
            <VisualController changeDimension={this.changeDimension} changeColor={this.changeColor} changeScale={this.changeScale} />
          </ModuleContainer>
          <ModuleContainer title={intl.get('LAYER_OPTIONS')} close="true">
            <Layer changeLayer={this.changeLayer} />
          </ModuleContainer>
          <ModuleContainer title="属性选择" autowidth="true" hidden={layer === 1}>
            <NanoCharts />
          </ModuleContainer>
          <ModuleContainer title="时间统计" autowidth="true" dark="true" hidden={layer === 1}>
            <DateStatistic />
          </ModuleContainer>
        </div>
        <div className="right-moudles">
          <ModuleContainer right="true" title={intl.get('DATA_SOURCE')} close="true">
            <DataSource updateDataName={this.updateDataName} changeDataMap={this.changeDataMap} titleName={intl.get('DATA_SOURCE')}/>
          </ModuleContainer>
          <ModuleContainer right="true" title={intl.get('DATA_SOURCE_2')} hidden={this.state.params.KType === 'Cross'? false : true}>
            <DataSource updateDataName={this.updateDataName_2} changeDataMap={this.changeDataMap_2} titleName={intl.get('DATA_SOURCE_2')}/>
          </ModuleContainer>
          <ModuleContainer right="true" title={intl.get('ST_RANGE')} close="true">
            <DataRange DataMap={this.state.DataMap}/>
          </ModuleContainer>
          <ModuleContainer right="true" title={intl.get('PARAMETER_SETTINGS')} >
            <Parameter updateParams={this.updateParams} DataMap={this.state.DataMap}/>
          </ModuleContainer>
          <ModuleContainer right="true" title={intl.get('COMPUTING_INFO')} >
            <CalcuInfo params={this.state.params} getCalResult={this.getCalResult} />
          </ModuleContainer>
          <ModuleContainer right="true" title={intl.get('RESULT_DISPLAY')} close="true" autowidth="true">
            <Result calResult={this.state.calResult} />
          </ModuleContainer>
        </div>
      </div>
    );
  }
}
