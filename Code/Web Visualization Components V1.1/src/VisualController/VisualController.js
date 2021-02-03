import React from 'react'
import { Slider, Switch, Select } from 'antd';
import './VisualController.css';
import icon2d from './img/2d.png';
import icon3d from './img/3d.png';
import intl from 'react-intl-universal';

export default class VisualController extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      attriList:[],
      isRShow: true,
      isGShow: true,
      isBShow: true,
      RContent: 0,
      GContent: 1,
      BContent: 2
    };
  }
  componentDidUpdate(prevProps){
    if(prevProps.DataMap !== this.props.DataMap)
    {
      this.setState({attriList:this.props.DataMap['attriList']})
    }
  }
  changeDimension = (isChecked) => {
    this.props.changeDimension(isChecked);
  }

  changeColor = () => {
    const { isRShow, isGShow, isBShow, RContent, GContent, BContent } = this.state;
    this.props.changeColor({ isRShow, isGShow, isBShow, RContent, GContent, BContent });
    // console.log(this.state)
  }

  changeRS = (isChecked) => {
    this.setState({ isRShow: isChecked }, this.changeColor);
  }
  changeGS = (isChecked) => {
    this.setState({ isGShow: isChecked }, this.changeColor);
  }
  changeBS = (isChecked) => {
    this.setState({ isBShow: isChecked }, this.changeColor);
  }

  changeScale = (value) => {
    this.props.changeScale(value);
  }

  changeRContent = (index) => {
    for (var i=0;i<this.state.attriList.length;i++)
    {
      if(index==this.state.attriList[i])
      {
        this.setState({ RContent: i }, this.changeColor);break;
      }
    }
  }

  changeGContent = (index) => {
    for (var i=0;i<this.state.attriList.length;i++)
    {
      if(index==this.state.attriList[i])
      {
        this.setState({ GContent: i }, this.changeColor);break;
      }
    }
  }

  changeBContent = (index) => {
    for (var i=0;i<this.state.attriList.length;i++)
    {
      if(index==this.state.attriList[i])
      {
        this.setState({ BContent: i }, this.changeColor);break;
      }
    }
  }

  render() {
    // 获取滑块范围
    const slideParam = {
      max: 18,
      min: 10,
      step: 1,
    };
    var attriList = this.state.attriList;
    return <div>
      <h3 style={{"font-size":"12pt"}}>{intl.get('POI_DISPLAY_MODULE')}</h3>
      <div className="option-name">{intl.get('SCALE_OPTIONS')}</div>
      <Slider className="module-slider" max={slideParam.max} min={slideParam.min} step={slideParam.step} defaultValue={14}
        onChange={this.changeScale} />
      <div className="option-name">{intl.get('COLOR_SETTINGS')}</div>
      <div className="color-form">
        <Switch className="red" checkedChildren={intl.get('RED')} unCheckedChildren={intl.get('RED')} defaultChecked onChange={this.changeRS} />
        <Select className="attri-select" placeholder={attriList[0]} onChange={index => { this.changeRContent(index) }}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br />
        <Switch className="green" checkedChildren={intl.get('GREEN')} unCheckedChildren={intl.get('GREEN')} defaultChecked onChange={this.changeGS} />
        <Select className="attri-select" placeholder={attriList[1]} onChange={index => { this.changeGContent(index) }}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br />
        <Switch className="blue" checkedChildren={intl.get('BLUE')} unCheckedChildren={intl.get('BLUE')} defaultChecked onChange={this.changeBS} />
        <Select className="attri-select" placeholder={attriList[2]} onChange={index => { this.changeBContent(index) }}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br />
      </div>
      <div style={{ margin: "100px 0 0 5px" }} className="option-name">{intl.get('DIMENSION_OPTIONS')}</div>
      <img src={icon2d} alt="2d icon" className="icon2d" />
      <Switch className="dimension" checkedChildren="3D" unCheckedChildren="2D" defaultChecked onChange={this.changeDimension} />
      <img src={icon3d} alt="3d icon" className="icon3d" />
    </div>
  }
}