import React from 'react';
// import 'antd/dist/antd.css';
import './Parameter.css';
import { Select } from 'antd';
import { InputNumber } from 'antd';


export default class Parameter extends React.Component {
    state={
        CK_isdisabled: true,
        STK_isdisabled: true,
    };
      
    handleChange(value) {
        if(value==="Cross_K"){
            this.setState({CK_isdisabled: false});
        }else{
            this.setState({CK_isdisabled: true});
        }  
        if(value==="ST_K"){
            this.setState({STK_isdisabled: false});
        }else{
            this.setState({STK_isdisabled: true});
        }  
    }
    
    render() {
        const { Option } = Select;
        const {CK_isdisabled} = this.state;
        const {STK_isdisabled} = this.state;
    return(     
        <div>
            <h3>K函数参数设置</h3>
            <div className="Parameter">
            K函数类别
            <Select defaultValue="S_K"  className="Parameter-Select"  onChange={this.handleChange.bind(this)} >
                <Option value="Cross_K">交叉K函数</Option>
                <Option value="ST_K">时空K函数</Option>
                <Option value="S_K" >空间K函数</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">入参
            <Select defaultValue="one"  className="Parameter-Select" >
                <Option value="one">第一产业</Option>
                <Option value="two">第二产业</Option>
                <Option value="three" >第三产业</Option>
            </Select>
            <Select defaultValue="one" disabled={CK_isdisabled} className="Parameter-Unit-Select">
                <Option value="one">第一产业</Option>
                <Option value="two">第二产业</Option>
                <Option value="three" >第三产业</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">最大空间距离
            <InputNumber min={1} max={100000} defaultValue={2000}  className="Parameter-Select"/>
            <Select defaultValue="m"  className="Parameter-Unit-Select">
                <Option value="m">m</Option>
                <Option value="km">km</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">最大时间范围
            <InputNumber min={1} max={100} defaultValue={10} disabled={STK_isdisabled} className="Parameter-Select"/>
            <Select defaultValue="month" disabled={STK_isdisabled} className="Parameter-Unit-Select">
                <Option value="day">day</Option>
                <Option value="month">month</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">空间步长
            <InputNumber min={1} max={100000} defaultValue={20} className="Parameter-Select" />
            <Select defaultValue="m" className="Parameter-Unit-Select" >
                <Option value="m">m</Option>
                <Option value="km">km</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">时间步长
            <InputNumber min={1} max={100000} defaultValue={1} disabled={STK_isdisabled} className="Parameter-Select" />
            <Select defaultValue="month"  disabled={STK_isdisabled} className="Parameter-Unit-Select" >
                <Option value="day">day</Option>
                <Option value="month">month</Option>
            </Select>
            </div>
            <p></p>
            <div className="Parameter">模拟次数
            <InputNumber min={1} max={1000} defaultValue={100} className="Parameter-Select" />
            </div>
            <p></p>
        </div>
    );
  }
}

