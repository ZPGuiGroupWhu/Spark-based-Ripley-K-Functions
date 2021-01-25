import React from 'react';
import './DataSource.css';
import { Select, Upload, Button, Icon } from 'antd';
import intl from 'react-intl-universal';
import { getURLWithParam } from '../common/tool';
import LocalDataMap from './datamap.json'

export default class DataSource extends React.Component {
  constructor(props) {
    super(props);
    this.state = {dataSources: ['chongqing.csv','hubei.csv']};

//TODO 测试用代码，记得删除
    // this.changeDataSource('chongqing.csv');



    new Promise(resolve => {
      fetch('http://localhost:8080/spark/DataSource.do')
      .then((response) => response.json())
      .then((responseJson) => {
        resolve(responseJson);
      })
      .catch((error) => {
      console.error('请求数据列表出错', error);
      });
    }).then(res => {
      var i=0;
      var DataArray=new Array();
      for (var every in res)
      {
        DataArray[i]=res[every].name;
        i++;
      }
      this.setState({
        dataSources: DataArray
      });
      this.changeDataSource(DataArray[0]);
    })
  }

  changeDataSource = (index) => {
    this.props.updateDataName(index);
    const commitParam = {
      DataName: index,
    };

    //TODO 离线版本用LocalDataMap
    // this.props.changeDataMap(LocalDataMap[index]);

    const url = 'http://localhost:8080/spark/SetDataSource.do';
    const urlParam = getURLWithParam(url, commitParam);
    fetch(urlParam)
    .then((response) => response.json())
    .then((responseJson) => {
        this.props.changeDataMap(responseJson);
    })
    .catch((error) => {
    console.error('设置数据出错', error);
    });
  }

  render() {

    // 上传
    // const props = {
    //   name: 'file',
    //   action: 'https://www.mocky.io/v2/5cc8019d300000980a055e76',
    //   headers: {
    //     authorization: 'authorization-text',
    //   },
    //   onChange(info) {
    //     if (info.file.status !== 'uploading') {
    //       console.log(info.file, info.fileList);
    //     }
    //     if (info.file.status === 'done') {
    //       message.success(`${info.file.name} file uploaded successfully`);
    //     } else if (info.file.status === 'error') {
    //       message.error(`${info.file.name} file upload failed.`);
    //     }
    //   },
    // };

    return <div>
      <h3>{this.props.titleName}</h3>
      <div className="data-option">{intl.get('SELECT_DATA')}
        <Select className="data-source-button" placeholder={this.state.dataSources[0]} onChange={index => { this.changeDataSource(index) }}>
          {
            this.state.dataSources.length && this.state.dataSources.map((item, index) => (
              <Select.Option key={index} value={item}>{item}</Select.Option>)
            )
          }
        </Select>
      </div>
      <div className="data-option">{intl.get('UPLOAD_DATA')}
        <Upload className="data-source-button" >
          <Button style={{ width: '150px' }}>
            <Icon type="upload" /> Click to Upload
            </Button>//
        </Upload>
      </div>
    </div>//增加边界选择下拉框以及两个上传的按钮


//GeoCommer部署一个在遥感院的局域网上，使用SVN进行版本管理
//自定义时间不能超过时间边界
//选择第二数据源时，弹出第二数据源选择div
//数据选项之间互斥（第一产业选中后，第二个参数中不可再选中第一产业）
  }
}
