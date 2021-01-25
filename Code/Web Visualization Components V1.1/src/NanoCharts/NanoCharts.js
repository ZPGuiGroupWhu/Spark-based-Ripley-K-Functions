import React from 'react';
import './NanoCharts.css';

export default class NanoCharts extends React.Component {
  render() {
    return <div className="charts-container">
        <div id="Industry" className="lv1-box"></div>
        <div id="Type" className="type-box"></div>
      </div>
  }
}