import React from 'react';
// import 'antd/dist/antd.css';
import './Parameter.css';
import { Select } from 'antd';
import { InputNumber } from 'antd';
import intl from 'react-intl-universal';

let timer = null;

export default class Parameter extends React.Component {
	constructor(props) {
		super(props);
		this.state = {
			dataAttri:['第一类别','第二类别'],
			KType: 'ST',
			DataCate1: 0,
			DataCate2: 1,
			SpatialMax: 2000,
			TimeMax: 20,
			SpatialStep: 20,
			TimeStep: 20,
			simuTime: 1,
		};
	}
	// KType: S/ST/Cross
	componentDidUpdate(prevProps) {
		if (prevProps.DataMap !== this.props.DataMap) {
			var Setting=this.props.DataMap["setting"];
			this.setState({dataAttri:this.props.DataMap['attriList']})
			const {maxSpatialDistance, maxTemporalDistance, simuTime, spatialStep, temporalStep } = Setting;
			// this.setState({'SpatialMax':maxSpatialDistance,'TimeMax':maxTemporalDistance,'SpatialStep':spatialStep,'TimeStep':temporalStep,'simuTime':simuTime});
			
			this.changeParams(maxSpatialDistance,'SpatialMax');
			this.changeParams(maxTemporalDistance,'TimeMax');
			this.changeParams(spatialStep,'SpatialStep');
			this.changeParams(temporalStep,'TimeStep');
			this.changeParams(simuTime,'simuTime');
		}
	};


	pushData = () => {
		const { KType, DataCate1, DataCate2, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime } = this.state;
		let DataCate;
		if (KType === 'Cross') {
			DataCate = [DataCate1, DataCate2];
		} else {
			DataCate = DataCate1;
		}
		const params = { KType, DataCate, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime };
		this.props.updateParams(params);
	}

	debouncePushData = () => {
		const that = this;
		if (timer) {
			clearTimeout(timer);
		}
		timer = setTimeout(that.pushData, 300);
	}

	changeParams = (value, key) => {
		this.setState({ [key]: value }, this.debouncePushData);
	}

	render() {
		const { Option } = Select;
		return (
			<div>
				<h3 style={{"font-size":"12pt"}}>{intl.get('KFUNCTION_PARAMETER')}</h3>
				<div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('KFUNCTION_TYPE')}</div>
						<Select defaultValue="ST" className="Parameter-Select" onChange={value => { this.changeParams(value, 'KType') }} >
							<Option value="Cross">{intl.get('CROSS_K_FUNCTION')}</Option>
							<Option value="ST">{intl.get('ST_K_FUNCTION')}</Option>
							<Option value="L" >{intl.get('LOCAL_K_FUNCTION')}</Option>
							<Option value="S" >{intl.get('SPACE_K_FUNCTION')}</Option>
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('INPUT')}</div>
						<Select defaultValue={this.state.dataAttri[0]} className="Parameter-Select" onChange={value => { this.changeParams(value, 'DataCate1') }} >
							{
								this.state.dataAttri.length && this.state.dataAttri.map((item, index) => (
									<Option key={index} value={index}>{item}</Option>)
								)
							}
						</Select>
						<Select defaultValue={this.state.dataAttri[1]} disabled={this.state.KType !== 'Cross'} className="Parameter-Unit-Select" onChange={value => { this.changeParams(value, 'DataCate2') }}>
							{
								this.state.dataAttri.length && this.state.dataAttri.map((item, index) => (
									<Option key={index} value={index}>{item}</Option>)
								)
							}
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('MAX_SPATIAL_DISTANCE')}</div>
						<InputNumber min={1} max={100000} value={this.state.SpatialMax} className="Parameter-Select" onChange={value => { this.changeParams(value, 'SpatialMax') }} />
						<Select defaultValue="m" className="Parameter-Unit-Select">
							<Option value="m">km</Option>
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('MAX_TEMPORAL_DISTANCE')}</div>
						<InputNumber min={1} max={100} value={this.state.TimeMax} disabled={this.state.KType !== 'ST'} className="Parameter-Select" onChange={value => { this.changeParams(value, 'TimeMax') }} />
						<Select defaultValue="month" disabled={this.state.KType !== 'ST'} className="Parameter-Unit-Select">
							<Option value="month">month</Option>
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('SPACE_STEP')}</div>
						<InputNumber min={1} max={100000} value={this.state.SpatialStep} className="Parameter-Select" onChange={value => { this.changeParams(value, 'SpatialStep') }} />
						<Select defaultValue="m" className="Parameter-Unit-Select" >
							<Option value="m">km</Option>
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('TIME_STEP')}</div>
						<InputNumber min={1} max={100000} value={this.state.TimeStep} disabled={this.state.KType !== 'ST'} className="Parameter-Select" onChange={value => { this.changeParams(value, 'TimeStep') }} />
						<Select defaultValue="month" disabled={this.state.KType !== 'ST'} className="Parameter-Unit-Select" >
							<Option value="month">month</Option>
						</Select>
					</div>
					<div className="Parameter">
						<div className="paramter-title">{intl.get('SIMULATION_TIMES')}</div>
						<InputNumber id="test_input" min={1} max={1000} value={this.state.simuTime} className="Parameter-Select" onChange={value => { this.changeParams(value, 'simuTime') }} />
					</div>
				</div>
			</div>
		);
	}
}

