import React from 'react';
import './CalcuInfo.css';
import { Button, Progress, AutoComplete } from 'antd';
import { getURLWithParam } from '../common/tool';
import liquidfill from 'echarts-liquidfill';
import ReactEcharts from 'echarts-for-react';
import Scrollbars from 'react-custom-scrollbars';
import data_state from '../common/running.json';
//import 'function.js';
import H_S from '../common/hubei_S.json';
import H_ST from '../common/hubei_ST.json';
import C_S from '../common/result1.json';
import C_ST from '../common/chongqing_ST.json';



const CALCUSTATE = {
  BEFRORECALCU: 0,
  PARAMERROR: -1,
  CALCUING: 1,
  FINISHED: 2,
  SERVERERROR: -2,
};

const index = 'endInfo';
let Status_data = { ...data_state};
let loadJsonDataSuccess=false;

export default class CalcuInfo extends React.Component {
  constructor(props) {
    super(props);
    this.jsonDataCache = {}
    this.jsonDataCache2 = {}
    this.state = {
      app_id:"",
      app_time:null,
      calcuState: CALCUSTATE.BEFRORECALCU,
      resultJson:"",
      percent:0,
      flag:0,
      display_name:'none',
      Info_name:'endInfo',
      text_inside:'详 情',
      data2:[0.25],
      data22:[],
      per:0
    };
  };

  //获取初始状态json
  loadJsonData = () => {
    const url = "http://192.168.200.149:8011/monitoring/application?status=running";
	return fetch(url)
      .then((response) => response.json())
      .then((responseJson) => {
		console.log("cache1:",responseJson)
        loadJsonDataSuccess=false;
        this.jsonDataCache = responseJson[0];
		if(this.jsonDataCache == null)return ;
        let id_temp = this.jsonDataCache.id;
		console.log(id_temp)
        if(id_temp!==this.state.app_id)//当前运行任务与之前任务不相同,则更新信息
        {
          if(this.state.app_id!=="")//若之前任务非空
          {
            this.addFinishTask(this.state.app_id,this.state.app_time)//添加之前任务为已完成任务
			this.setState({percent:0});
          }
          this.setState({app_id:id_temp});
          //根据读取到的运行任务信息渲染
          document.getElementById("NodeCount").innerHTML="集群当前计算节点数：<span id='count'>asd</span>"
          document.getElementById("count").innerHTML = this.jsonDataCache.activeNodeCount;
          var taskCommitTime=new Date();
          this.setState({app_time:taskCommitTime});
          document.getElementById("runningTask").innerHTML="&nbsp;"+this.jsonDataCache.id.slice(-10)+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+(Array(2).join(0)+taskCommitTime.getHours()).slice(-2)+":"+(Array(2).join(0)+taskCommitTime.getMinutes()).slice(-2)+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
        }
		loadJsonDataSuccess=true;
        console.log("🚀 ~ file: CalcuInfo.js ~ line 75 ~ CalcuInfo ~ .then ~ loadJsonDataSuccess", loadJsonDataSuccess)
      });
    
  }
  //获取定时请求json
  loadJsonData2 = () => {
    //const url = "http://localhost:8011/monitoring/application?displayDetail=false&status=running";
    const url = "http://192.168.200.149:8011/monitoring/application?id="+this.state.app_id;
    return fetch(url)
      .then((response) => response.json())
      .then((responseJson) => {
		  //console.log("cache2:",responseJson)
        this.jsonDataCache2 = responseJson[0];
      });
    
  }

  // getJson(){
  //   $.ajax({
      
  //   })
  // }

  componentDidMount(){
    this.timer = setInterval(()=>{
      let yaxisData = Math.random();
      this.setState({
        data22:this.state.data2
      })
      this.state.data22.shift();
      this.setState(prevState => ({
        data2:prevState.data22.concat(yaxisData),
      }))
      this.loadJsonData2();

    },500)
  }

  shouldComponentUpdate(data2,nextState){
    if(data2 === this.state.data2){
      return false
    }
    return true
  }

  getOption(){
    
    let a = this.jsonDataCache2.completedRate;
    let b = parseFloat(a)/100;
    var c = [];
    c.push(b);

    return {
      series:[
        {
          name: '请求中',
          type: 'liquidFill',
          radius:'80%',
          center:['60%','50%'],
          subtext: '计算进度',
          outline:{show:false},
          backgroundStyle: {
            borderColor: '#156ACF',
            borderWidth: 3,
            shadowColor: 'rgba(0, 0, 0, 0.4)',
            shadowBlur: 20
        },
          waveAnimation:true,
          //data:this.state.per,
          data:c,
          //data:this.state.data2,
          //data:[0.6],
          // color:['#afb11b'],
          color:['rgb(22,85,180,0.65)'],
          itemStyle:{
            opacity:0.6
          },
          emphasis:{
            itemStyle:{
              opacity:0.9
            }
          },
          label: {
           
            fontSize: 18,
           // color: '#D94854'
        }
          //data:this.state.data2
        }
      ],

    }
  }

  startCalcu = () => {
    document.getElementById("details").style.display="block"; //------------------------
    document.getElementById("NodeCount").innerHTML="正在请求中，请稍候"
    const params = this.props.params;
    const {KType, DataName, DataName_2,DataCate, SpatialMax, TimeMax, SpatialStep, TimeStep, simuTime} = params;




    //TODO 根据K函数类型和数据名称选用相应的本地结果文件
    // var LocalresponseJson='';
    // if(KType=='ST')
    // {
    //   if(DataName=='hubei.csv')LocalresponseJson=H_ST;
    //   else LocalresponseJson=C_ST;
    // }
    // else
    // {
    //   if(DataName=='hubei.csv')LocalresponseJson=H_S;
    //   else LocalresponseJson=C_S;
    // }
    // this.props.getCalResult(LocalresponseJson);




    // if(KType === 'Cross'){
    //   if(params.DataCate[0] === params.DataCate[1]){
    //     alert('交叉K函数入参点数据类型不能相同');
    //     return false;
    //   }
    // }
    const commitParam = {
      maxSpatialDistance: SpatialMax,
      maxTemporalDistance: TimeMax,
      spatialStep: SpatialStep,
      temporalStep: TimeStep,
      simulationTimes: simuTime,
      ktype: KType,
      dataName: DataName,
      dataName2:DataName_2,
    };
    const url = 'http://192.168.200.149:8011/spark/submit/kfunction';
    console.log(JSON.stringify(commitParam));
    // 能正确请求到结果
    fetch(url,{
      method: 'POST', 
      body: JSON.stringify(commitParam),
	  headers: {'Content-Type': 'application/json'}})
   .then((response) => response.json())
   .then((responseJson) => {
     console.log("CalcuInfo -> startCalcu -> responseJson", responseJson)
     this.setState({resultJson:responseJson});
     console.log("结果json:"+responseJson);
     this.props.getCalResult(responseJson);
   })
   .catch((error) => {
    console.error('请求计算结果出错', error);
   });
    this.setState({ percent: 0,per:0}, this.changePercent);
    let i=0;
    var that=this;
    var interval = setInterval(function() {
		  console.log(i);
		  console.log(loadJsonDataSuccess);
      if(i < 100 && !loadJsonDataSuccess) {
         i++;
         that.loadJsonData();
      }
	    else{
        if(!loadJsonDataSuccess)
        {
          document.getElementById("NodeCount").innerHTML="请求失败！请重试"
        }
		    clearInterval(interval);
		    loadJsonDataSuccess=false;
	    }}, 3000);
    
  }
  addFinishTask(id,time){//添加已完成的任务
    var newSpan=document.createElement("span");
    newSpan.setAttribute("style","float:left")
    newSpan.innerHTML="&nbsp;"+id.slice(-10)+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+(Array(2).join(0)+time.getHours()).slice(-2)+":"+(Array(2).join(0)+time.getMinutes()).slice(-2)+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    document.getElementById("taskInfo").appendChild(newSpan);
    var newPro=document.createElement("span");
    newPro.innerHTML="<div class=\"ant-progress ant-progress-line ant-progress-status-success ant-progress-show-info ant-progress-default\" style=\"width: 35%; height: 3%; float: left;\"><div><div class=\"ant-progress-outer\"><div class=\"ant-progress-inner\"><div class=\"ant-progress-bg\" style=\"width: 100%; height: 8px;\"></div></div></div><span class=\"ant-progress-text\"><i aria-label=\"icon: check-circle\" class=\"anticon anticon-check-circle\"><svg viewBox=\"64 64 896 896\" focusable=\"false\" class=\"\" data-icon=\"check-circle\" width=\"1em\" height=\"1em\" fill=\"currentColor\" aria-hidden=\"true\"><path d=\"M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm193.5 301.7l-210.6 292a31.8 31.8 0 0 1-51.7 0L318.5 484.9c-3.8-5.3 0-12.7 6.5-12.7h46.9c10.2 0 19.9 4.9 25.9 13.3l71.2 98.8 157.2-218c6-8.3 15.6-13.3 25.9-13.3H699c6.5 0 10.3 7.4 6.5 12.7z\"></path></svg></i></span></div></div>";
    document.getElementById("taskInfo").appendChild(newPro);
    document.getElementById("taskInfo").appendChild(document.createElement("br"));
  }
  addNodeInfo(taskNodeInfo){//添加当前节点信息
    var nodeInfoDiv=document.getElementById("nodeInfo");
    nodeInfoDiv.innerHTML="";//首先删除之前的节点信息
    for(let nodeIndex in taskNodeInfo){
      let newSpan=document.createElement("span");
      newSpan.setAttribute("style","float:right");
      let nodeTime="00:00:00"
      if(taskNodeInfo[nodeIndex].addTime!=null)
      {
        nodeTime=taskNodeInfo[nodeIndex].addTime.slice(11,19);
      }
      newSpan.innerHTML="&nbsp;&nbsp;&nbsp;"+taskNodeInfo[nodeIndex].id+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+taskNodeInfo[nodeIndex].storageMemory+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+nodeTime+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";
      nodeInfoDiv.appendChild(newSpan);
      nodeInfoDiv.appendChild(document.createElement("br"));
    }
  }
  illJson(){
    var temp = this.state.per+1
    console.log(temp)
    this.setState({per:temp})
    
    return this.state.per

  }
  changePercent = () => {
	  //console.log("this.state.percent",this.state.percent)
    if(this.state.percent < 100 || isNaN(this.state.percent)) {
      setTimeout(()=>{
        // let a = Status_data.completedRate;
        let a = this.jsonDataCache2.completedRate;//真实情况应该用这个
        let b = parseFloat(a);
        this.setState({percent:b})
        this.addNodeInfo(this.jsonDataCache2.executorDetails)
        this.changePercent();
      }, 1000);
    } else{     
		setTimeout(()=>{
        this.changePercent();
      }, 1000);
      //this.props.getCalResult(this.state.resultJson);
    }
  }

  
  display_name(){

    var css = document.getElementById("css") ;
    //this.get_status();   
    if(this.state.display_name == 'none'){
      this.setState({
        display_name:'block',
        Info_name:'checkInfo',
        text_inside:'收起'
      });

    }
    else if(this.state.display_name == 'block'){
      this.setState({
        display_name:'none',
        Info_name:'endInfo',
        text_inside:'详 情 '
      })
    }
  }

  renderThumb({style,...props}){
    const thumbStyle={
      width:'8px',
      backgroundColor:'rgb(115,115,115,0.65)',
      opacity:'0.7',
      borderRadius:'3px',
      right:'2px'
    }
    return (
      <div
        style={{...style,...thumbStyle}}
        {...props}/>
    )
  }

  getInfo = () =>{
    this.state.flag = 1;
  }

  render() {
    
    return <div className="calcuInfo" style={{float:'none',width:290,display:'inline-block'}}>
        <h3 style={{"font-size":"12pt"}}>集群计算信息</h3>
        { 
          this.state.calcuState === CALCUSTATE.BEFRORECALCU &&
          <div>
            <Button onClick={this.startCalcu}>点击开始计算</Button>
            <div className="cal-progress" id="details" style={{float:'left',display:'none',marginLeft:'5px'}}>
              <div style={{float:'left'}}>
                <ReactEcharts
                option={this.getOption()} style={{width:80,height:70,float:'left',padding:'20 0 0 30'}}
                />
                <h3 id= "NodeCount"style={{height:60,width:120,float:'left',padding:'16px 0 0 35px'}}>集群当前计算节点数：<span id='count'>asd</span></h3>
                <br/>
            
              </div>
              <div style={{width: '280px',height:'25px',float:'left'}}>
                  <Button className={index===this.state.Info_name ? "endInfo" : "checkInfo"}  onClick={this.display_name.bind(this)}>{this.state.text_inside}</Button>
              </div>

              <div
                  style={{
                    float:'left',
                    width:230,
                    display:this.state.display_name
                  }}
              >
                {/* <Button onClick={this.getInfo}>查看详情</Button> */}
              
                <span style={{float:'left',padding:'4px 0 0 4px'}}>计算任务信息：</span>
                <span style={{float:'left',width:'290px',textAlign:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ID&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;开始时间&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;完成进度</span>
                
                <Scrollbars style={{height:60,width:'290px'}} renderThumbVertical={this.renderThumb}>
                <div id="taskInfo" style={{float:'left',height:'60px',width:'290px'}}>
                  <span id="runningTask" style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;0&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:00 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                  <span><Progress style={{width:'35%',height:'3%',float:'left'}} percent={this.state.percent}/></span><br/>
                  {/* <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:53 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                  <span><Progress style={{width:'35%',height:'3%',float:'left'}} percent={100}/></span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;6:26 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                  <span><Progress style={{width:'35%',height:'3%',float:'left'}} percent={100}/></span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;7:22 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                  <span><Progress style={{width:'35%',height:'3%',float:'left'}} percent={100}/></span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;7:59 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                  <span><Progress style={{width:'35%',height:'3%',float:'left'}} percent={100}/></span><br/> */}
                </div>
                </Scrollbars>

                <span style={{float:'left',padding:'4px 0 0 4px'}}>节点信息：</span>
                <span style={{float:'left',width:'290px',textAlign:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ID&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;储存内存&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;开始时间</span>
                <Scrollbars style={{height:60,width:'290px'}} renderThumbVertical={this.renderThumb}>
                <div id="nodeInfo" style={{float:'left',height:'60px',width:'290px'}}>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;driver&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.38k/384.09m &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:00</span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.52k/384.09m &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:00</span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.93k/384.09m &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:01</span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.71k/384.09m &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:00</span><br/>
                  <span style={{float:'left'}}>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;0&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.49k/384.09m &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:02</span><br/>
                </div>
                </Scrollbars>
                <div style={{float:'left',width:'290px',paddingTop:'2px'}}>
                  <h3 style={{float:'left',marginBottom:'0px',padding:'2px 0 0 0'}}>spark原生监控：</h3>
                  <div style={{float:'left',width:'290px',paddingTop:'2px'}}>
                    <span>
                    <Button className="linkb" style={{}} οnclick="window.open('http://192.168.50.131:8080')">任务监控UI</Button>
                    </span>
                  
                    <span>
                    <Button className="linkb" style={{}} οnclick="window.open('http://192.168.50.131:4040')">应用监控UI</Button>
                    </span>
                  </div>
                </div>
              </div> 
            </div>
          </div>
        }
  
      </div>
  }
}