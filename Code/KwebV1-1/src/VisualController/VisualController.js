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
      isRShow: true,
      isGShow: true,
      isBShow: true,
      RContent: 1,
      GContent: 2,
      BContent: 3
    };
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
    switch (index)
    {
      case '第一产业': this.setState({ RContent: 1 }, this.changeColor);break;
      case '第二产业': this.setState({ RContent: 2 }, this.changeColor);break;
      case '第三产业': this.setState({ RContent: 3 }, this.changeColor);break;
      default:break;
    }
  }

  changeGContent = (index) => {
    switch (index)
    {
      case '第一产业': this.setState({ GContent: 1 }, this.changeColor);break;
      case '第二产业': this.setState({ GContent: 2 }, this.changeColor);break;
      case '第三产业': this.setState({ GContent: 3 }, this.changeColor);break;
      default:break;
    }
  }

  changeBContent = (index) => {
    switch (index)
    {
      case '第一产业': this.setState({ BContent: 1 }, this.changeColor);break;
      case '第二产业': this.setState({ BContent: 2 }, this.changeColor);break;
      case '第三产业': this.setState({ BContent: 3 }, this.changeColor);break;
      default:break;
    }
  }

  render() {
    // 获取滑块范围
    const slideParam = {
      max: 18,
      min: 10,
      step: 1,
    };
    const attriList = ['第一产业', '第二产业', '第三产业'];
    return <div>
      <h3>{intl.get('POI_DISPLAY_MODULE')}</h3>
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