import React from 'react';
import { Radio } from 'antd';
import './Layer.css';

export default class Layer extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      value: 2,
    };
  };

  onChange = e => {
    this.setState({
      value: e.target.value,
    });
    this.props.changeLayer(e.target.value);
  };

  render() {
    const radioStyle = {
      display: 'block',
      height: '30px',
      lineHeight: '30px',
      color: 'white',
      textAlign: 'left',
    };
    return (
      <div>
        <h3>图层选择</h3>
        <Radio.Group onChange={this.onChange} value={this.state.value}>
          <Radio style={radioStyle} value={1}>
            Deckgl
          </Radio>
          <Radio style={radioStyle} value={2}>
            Nanocube
          </Radio>
        </Radio.Group>
      </div>
    );
  }
}