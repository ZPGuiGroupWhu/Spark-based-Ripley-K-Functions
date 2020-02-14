import React from 'react'
import { Slider, Switch, Select } from 'antd';
import './VisualController.css';
import icon2d from './img/2d.png';
import icon3d from './img/3d.png';

export default class VisualController extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      isRShow: true,
      isGShow: true,
      isBShow: true,
      RContent: 1,
      GContent: 2,
      BContent: 3,
    };
  }

  changeDimension = (isChecked) => {
    this.props.changeDimension(isChecked);
  }

  changeColor = () => {
    const {isRShow, isGShow, isBShow, RContent, GContent, BContent} = this.state;
    this.props.changeColor({isRShow, isGShow, isBShow, RContent, GContent, BContent});
  }

  changeRS = (isChecked) => {
    this.setState({isRShow: isChecked}, this.changeColor);
  }
  changeGS = (isChecked) => {
    this.setState({isGShow: isChecked}, this.changeColor);
  }
  changeBS = (isChecked) => {
    this.setState({isBShow: isChecked}, this.changeColor);
  }


  render() {
    // 获取滑块范围
    const slideParam = {
      max: 20,
      min: 1,
      step: 1,
    };
    const attriList = ['第一产业', '第二产业', '第三产业'];
    return <div>
      <h3>点数据展示模块</h3>
      <div className="option-name">尺度选择</div>
      <Slider className="module-slider" max={slideParam.max} min={slideParam.min} step={slideParam.step} defaultValue={10}/>
      <div className="option-name">颜色设置</div>
      <div className="color-form">
        <Switch className="red" checkedChildren="红" unCheckedChildren="红" defaultChecked onChange={this.changeRS}/>
        <Select className="attri-select" placeholder={attriList[0]}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br/>
        <Switch className="green" checkedChildren="绿" unCheckedChildren="绿" defaultChecked onChange={this.changeGS}/>
        <Select className="attri-select" placeholder={attriList[1]}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br/>
        <Switch className="blue" checkedChildren="蓝" unCheckedChildren="蓝" defaultChecked onChange={this.changeBS}/>
        <Select className="attri-select" placeholder={attriList[2]}>
          {
            attriList.length && attriList.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select><br/>
      </div>
      <div style={{margin: "100px 0 0 5px"}} className="option-name">维度选择</div>
      <img src={icon2d} alt="2d icon" className="icon2d"/>
      <Switch className="dimension" checkedChildren="3D" unCheckedChildren="2D" defaultChecked onChange={this.changeDimension}/>
      <img src={icon3d} alt="3d icon" className="icon3d"/>
    </div>
  }
}