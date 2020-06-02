import React from 'react';
// import 'antd/dist/antd.css';
import './Parameter.css';
import { Select } from 'antd';
import { InputNumber } from 'antd';

const dataAttri = ['第一产业', '第二产业', '第三产业'];
let timer = null;

export default class Parameter extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            KType: 'S',
            DataCate1: 0,
            DataCate2: 1,
            SpatialMax: 2000,
            TimeMax: 20,
            SpatialStep: 20,
            TimeStep: 20,
            simuTime: 100,
        };
      }
    // KType: S/ST/Cross
    
      
    pushData = () => {
        const {KType, DataCate1, DataCate2, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime} = this.state;
        let DataCate;
        if(KType === 'Cross'){
            DataCate = [DataCate1, DataCate2];
        } else {
            DataCate = DataCate1;
        }
        const params = {KType, DataCate, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime};
        this.props.updateParams(params);
    }

    debouncePushData = () => {
        const that = this;
        if(timer){
            clearTimeout(timer);
        }
        timer = setTimeout(that.pushData, 1000);
    }

    changeParams = (value, key) => {
        this.setState({ [key]: value }, this.debouncePushData);
    }
    
    render() {
        const { Option } = Select;
        
        return(     
            <div>
                <h3>K函数参数设置</h3>
                <div className="Parameter">
                K函数类别
                <Select defaultValue="S"  className="Parameter-Select"  onChange = {value=>{this.changeParams(value,'KType')}} >
                    <Option value="Cross">交叉K函数</Option>
                    <Option value="ST">时空K函数</Option>
                    <Option value="S" >空间K函数</Option>
                </Select>
                </div>
                <p></p>
                <div className="Parameter">入参
                <Select defaultValue={dataAttri[0]}  className="Parameter-Select" onChange = {value=>{this.changeParams(value,'DataCate1')}} >
                {
                    dataAttri.length && dataAttri.map((item, index) => (
                    <Option key={index} value={index}>{item}</Option>)
                    )
                }
                </Select>
                <Select defaultValue={dataAttri[1]} disabled={this.state.KType !== 'Cross'} className="Parameter-Unit-Select" onChange = {value=>{this.changeParams(value,'DataCate2')}}>
                {
                    dataAttri.length && dataAttri.map((item, index) => (
                    <Option key={index} value={index}>{item}</Option>)
                    )
                }
                </Select>
                </div>
                <p></p>
                <div className="Parameter">最大空间距离
                <InputNumber min={1} max={100000} defaultValue={2000}  className="Parameter-Select" onChange = {value=>{this.changeParams(value,'SpatialMax')}} />
                <Select defaultValue="m"  className="Parameter-Unit-Select">
                    <Option value="m">km</Option>
                </Select>
                </div>
                <p></p>
                <div className="Parameter">最大时间范围
                <InputNumber min={1} max={100} defaultValue={10} disabled={this.state.KType !== 'ST'} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'TimeMax')}} />
                <Select defaultValue="month" disabled={this.state.KType !== 'ST'} className="Parameter-Unit-Select">
                    <Option value="month">month</Option>
                </Select>
                </div>
                <p></p>
                <div className="Parameter">空间步长
                <InputNumber min={1} max={100000} defaultValue={20} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'SpatialStep')}}/>
                <Select defaultValue="m" className="Parameter-Unit-Select" >
                    <Option value="m">km</Option>
                </Select>
                </div>
                <p></p>
                <div className="Parameter">时间步长
                <InputNumber min={1} max={100000} defaultValue={1} disabled={this.state.KType !== 'ST'} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'TimeStep')}}/>
                <Select defaultValue="month"  disabled={this.state.KType !== 'ST'} className="Parameter-Unit-Select" >
                    <Option value="month">month</Option>
                </Select>
                </div>
                <p></p>
                <div className="Parameter">模拟次数
                <InputNumber min={1} max={1000} defaultValue={100} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'simuTime')}}/>
                </div>
                <p></p>
                <div className="Parameter">节点个数
                <InputNumber min={1} max={8} defaultValue={8} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'nodeNum')}}/>
                </div>
                <p></p>
                <div className="Parameter">内存大小(G)
                <InputNumber min={1} max={16} defaultValue={8} className="Parameter-Select" onChange = {value=>{this.changeParams(value,'romSize')}}/>
                </div>
                <p></p>
            </div>
        );
  }
}

