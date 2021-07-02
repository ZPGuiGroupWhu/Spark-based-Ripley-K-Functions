import React from 'react';
import './DataRange.css';
import { Select, Radio, DatePicker,Checkbox } from 'antd';
import intl from 'react-intl-universal';
import moment from 'moment';

const { RangePicker } = DatePicker;


export default class DataRange extends React.Component {
  constructor(props) {
		super(props);
		this.state = {
      RangeSelect:false,
      TimeRangeSelect:false,
      SRange:'',
      TRange:null,
		};
	}
  componentDidUpdate(prevProps) {
		if (prevProps.DataMap !== this.props.DataMap) {
      this.setState({SRange:this.props.DataMap['boundaryPath']});
      this.setState({TRange:[moment(this.props.DataMap['TimeStart'], 'YYYY/MM/DD'),moment(this.props.DataMap['TimeEnd'], 'YYYY/MM/DD')]});
		}
	};
  onCheckChange= (index) => {
    this.setState({'RangeSelect':index.target.checked});
    if(!index.target.checked)
    {
      if(this.props.DataMap!=null)this.setState({SRange:this.props.DataMap['boundaryPath']});
    }
  }

  onTimeCheckChange= (index) => {
    this.setState({'TimeRangeSelect':index.target.checked});
    if(!index.target.checked)
    {
      if(this.props.DataMap!=null)this.setState({TRange:[moment(this.props.DataMap['TimeStart'], 'YYYY/MM/DD'),moment(this.props.DataMap['TimeEnd'], 'YYYY/MM/DD')]});
    }
  }

  render() {
    const subRanges = ['重庆', '湖北', '北京', '天津', '上海', '四川', '...'];
    const radioStyle = {
      display: 'block',
      height: '32px',
      lineHeight: '20px',
      float: 'left',
      marginLeft: '5px',
    };
    const dateFormat = 'YYYY/MM/DD';
    return <div className="data-range">
      <h3 style={{"fontSize":"12pt"}}>{intl.get('ST_RANGE')}</h3>
      <div className="option-name">{intl.get('GEO_EXTENT')}</div><br /><br />
      {/* <Radio.Group > */}
        <Checkbox onChange={index => { this.onCheckChange(index) }} style={radioStyle}>
          <span className='radio-text'>{intl.get('CUSTOMIZED')}</span>
          <Select className="data-select" value={this.state.SRange} onChange={value => { this.setState({SRange: value});}} disabled={!this.state.RangeSelect} >
            {
              subRanges.length && subRanges.map((item, index) => (
                <Select.Option key={index} value={item}>{item}</Select.Option>)
              )
            }
          </Select>
        </Checkbox>
        {/* <Radio style={radioStyle} value={2}>
          <span className='radio-text'>{intl.get('DEFAULT')}</span>
        </Radio> */}
      {/* </Radio.Group><br /> */}
      <br /><br />
      <div className="option-name">{intl.get('TIME_RANGE')}</div><br /><br />
      {/* <Radio.Group > */}
        <Checkbox onChange={index => { this.onTimeCheckChange(index) }} style={radioStyle}>
          <span className='radio-text'>{intl.get('CUSTOMIZED')}</span>
          <RangePicker style={{ width: '178px', marginLeft: '3px' }} value={this.state.TRange} format={dateFormat} onChange={value => { this.setState({TRange: value}) }} disabled={!this.state.TimeRangeSelect}/>
        </Checkbox>
        {/* <Radio style={radioStyle} value={2}>
          <span className='radio-text'>{intl.get('DEFAULT')}</span>
        </Radio> */}
      {/* </Radio.Group> */}

    </div>
  }
}
