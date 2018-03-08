var isIE=(navigator.appName.indexOf("Microsoft")!=-1);
var isMoz=(navigator.appName.indexOf("Netscape")!=-1);
function oo(obj){return document.getElementById(obj)}
var bt=new Array()
function ov(obj){
	if(!bt[obj])
		return
	if(isIE){
		if(oo(obj).filters.alpha.opacity!=0){
			oo(obj).filters.alpha.opacity-=5
			window.setTimeout("ov('"+obj+"')",20)
		}
	}else{
		if(oo(obj).style.MozOpacity>0){
			oo(obj).style.MozOpacity-=0.1
			window.setTimeout("ov('"+obj+"')",200)
		}
	}
}
function out(obj){
	if(bt[obj])
		return
	if(isIE){
		if(oo(obj).filters.alpha.opacity!=100){
			oo(obj).filters.alpha.opacity+=5
			window.setTimeout("out('"+obj+"')",10)
		}
	}else{
		if(oo(obj).style.MozOpacity<1.0){
			if(oo(obj).style.MozOpacity<=0)
				oo(obj).style.MozOpacity=0.1
			oo(obj).style.MozOpacity=parseFloat(oo(obj).style.MozOpacity)+0.1
			window.setTimeout("out('"+obj+"')",100)
		}
	}
}