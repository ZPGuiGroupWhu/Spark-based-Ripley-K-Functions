/*global $,d3 */

// translate Chinese
var CN_CAT = {"Industry":"企业类型","Type":"经济实体类型"};
var My_TYPE = {"a":"个体", "f":"小微", "b":"内资", "e":"外资", "d":"受资助小微", "c":"农专"}
function MultiDonutChart(name){
    var margin = {top:20,right:10,left:30,bottom:30};
    var id = '#'+name;

    //setup the d3 margins from the css margin variables
    margin.left = Math.max(margin.left, parseInt($(id).css('margin-left')));
    margin.right = Math.max(margin.right, parseInt($(id).css('margin-right')));
    margin.top = Math.max(margin.top, parseInt($(id).css('margin-top')));
    margin.bottom = Math.max(margin.bottom,
                             parseInt($(id).css('margin-bottom')));

    $(id).css('margin','0px 0px 0px 0px');
    $(id).css('margin-left','0px');
    $(id).css('margin-right','0px');
    $(id).css('margin-top','0px');
    $(id).css('margin-bottom','0px');

    this.data = {};
    this.selection=null;

    this.id = id;

    var width = $(id).width() - margin.left - margin.right;
    var height = $(id).height()- margin.top - margin.bottom;

    //add svg to the div
    this.svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g").attr("transform","translate(" + $(id).width()/2 + ","
                          + $(id).height()/2 + ")");

    //add title
    var title = this.svg.append("text").attr('x', -$(id).width()/2 + 60).attr('y', -$(id).height()/2 + 15).text(CN_CAT[name])
	              .append("svg:title").text(function(d) { return "点击切换排序:按类别首字母/按类别数值由大至小"; });
}

MultiDonutChart.prototype.setData = function(data,id,color){
    this.data[id] = {color:color, data: data};
};

MultiDonutChart.prototype.setSelection = function(sel){
    this.selection = sel;
};

MultiDonutChart.prototype.removeData = function(id){
    if (id in this.data){ delete this.data[id]; }
};

MultiDonutChart.prototype.flattenData = function(data){
    var result = [];
    for (index in data)
    {
        var row = Object.keys(data[index].data).map(function(k){
            return { addr: data[index].data[k].addr,
                     cat: data[index].data[k].cat,
                     color: data[index].color,
                     value:data[index].data[k].value };
        });

        result.push(row);
    }

    return result;
};


MultiDonutChart.prototype.setClickCallback = function(cb){
    this.click_callback = cb;
};

MultiDonutChart.prototype.redraw = function(){
    var flatdata = this.flattenData(this.data);
    var that = this;

    var padAngle = 0;
    var pie = d3.layout.pie()
              .sort(null).value(function(d) {
                return d.value; //since score is the parameter for the pie
              })
              .padAngle(padAngle);
    var arc = d3.svg.arc();

    var width = $(this.id).width();
    var height = $(this.id).height();
    var CatCount = flatdata.length;

    this.svg.on('click', this.click_callback);

    //remove the tooltip
    d3.select("body").selectAll('.DonutTooltip').remove();

    //add the donuts tooltip
    this.tooltip = d3.select("body").append("div").attr("class", "DonutTooltip");

    //remove the donuts
    this.svg.selectAll('.donut')
        .data([])
        .exit()
        .remove();

    //add the donuts back
    this.donuts = this.svg.selectAll('.donut')
        .data(flatdata).enter()
        .append('g')
        .attr('class', 'donut');

    this.path = this.donuts.selectAll('path')
        .data(function(d) {
          return pie(d);
        })
        .enter().append('path')
        .attr('d', function(d, i, j) {
          var inrad = Math.min(width, height)*(2/8);
          var outrad = Math.min(width, height)*(3/8);
          if (CatCount > 1)
          {
            inrad = Math.min(width, height)*(1/8) + Math.min(width, height)*9/(32*(2*CatCount-1)) * (2*j);
            outrad = Math.min(width, height)*(1/8) + Math.min(width, height)*9/(32*(2*CatCount-1)) * (2*j+1);
          }
          var cornerRadius = 0;
          return arc.innerRadius(inrad).outerRadius(outrad).cornerRadius(cornerRadius)(d);
        })
        .on('click', this.click_callback)
        .attr('fill', function(d) {
          if (that.selection.length < 1  //no selection (everything)
              || (that.selection.indexOf(d.data.addr)!=-1)){//addr in selection
              return d.data.color;
          }
          else{
              return 'gray';
          }
        })
        .style('opacity', function(d) {
          return 1/(d.data.addr+1);
        });
    this.path.on('mousemove', function(d, i, j){
        var tooltip = d3.select("body").selectAll('.DonutTooltip');
        var ratio = (d.endAngle-d.startAngle)/(2*Math.PI)*100;
        tooltip.style("left", d3.event.pageX+10+"px");
        tooltip.style("top", d3.event.pageY-25+"px");
        tooltip.style("display", "inline-block");
        tooltip.html("<span style='font-weight: bold; color: "+d.data.color+"'>"+d.data.cat+"</span><br>数目: "+d.data.value+"<br>占比: "+ ratio.toFixed(3) +"%");
    });
    this.path.on('mouseout', function(d){
        var tooltip = d3.select("body").selectAll('.DonutTooltip');
        tooltip.style("display", "none");
    });
};
