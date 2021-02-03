import React from 'react';
import { Radio } from 'antd';
import './Layer.css';
import intl from 'react-intl-universal';

export default class Layer extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      value: 1,
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
        <h3 style={{"font-size":"12pt"}}>{intl.get('LAYER_OPTIONS')}</h3>
        <Radio.Group onChange={this.onChange} value={this.state.value}>
          <Radio style={radioStyle} value={1}>
            Deckgl
          </Radio>
          {/* <Radio style={radioStyle} value={2}>
            Nanocube
          </Radio> */}
        </Radio.Group>
      </div>
    );
  }
}