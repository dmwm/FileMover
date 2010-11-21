function loadMasthead() {
    try {
       insertMastHead('filemover','')
    } catch(err) {
//    txt="There was an error during masthead loading.\n\n";
//    txt+="Error description: " + err.description + "\n\n";
//    txt+="Click OK to continue.\n\n";
//    alert(txt);
    }
}
function footerMenuText(){
    return [
{label: "PhEDEx Home", link: "https://cmsweb.cern.ch/phedex/", title: "Data placement, transfer monitoring"},
{label: "FileMover", link: "https://cmsweb.cern.ch/filemover/", title: "Fetch your favorite LFN"},

    ]
}

var dbsUpdater=Class.create();
dbsUpdater.prototype = {
    initialize: function(tab) {
       this.tab=tab
    },
    ajaxUpdate: function(ajaxResponse) {
       var responseHTML=RicoUtil.getContentAsString(ajaxResponse);
       var t=document.getElementById(this.tab);
       var i=t.innerHTML;
       if (i && i.indexOf('loading.gif') != -1) {
           t.innerHTML=responseHTML;
           // parse response and search for any JavaScript code there, if found execute it.
           var jsCode = SearchForJSCode(responseHTML);
           if (jsCode) {
               eval(jsCode);
           }
       } else if (i && i.indexOf('Remove') != -1) {
           t.innerHTML=responseHTML;
       }
    }
}
var Updater=Class.create();
Updater.prototype = {
    initialize: function(tab) {
       this.tab=tab
    },
    ajaxUpdate: function(ajaxResponse) {
       var responseHTML=RicoUtil.getContentAsString(ajaxResponse);
       var t=document.getElementById(this.tab);
       t.innerHTML=responseHTML;
       // parse response and search for any JavaScript code there, if found execute it.
       var jsCode = SearchForJSCode(responseHTML);
       if (jsCode) {
           eval(jsCode);
       }
    }
}
function SearchForJSCode(text) {
    var pattern1='<script type="text\/javascript">';
    var pattern2='<script type=\'text\/javascript\'>';
    var end='<\/script>';
    var foundCode=SearchForCode(text,pattern1,end);
    foundCode=foundCode+SearchForCode(text,pattern2,end);
    return foundCode;
}
function SearchForCode(text,begPattern,endPattern) {
    var foundCode='';
    while( text && text.search(begPattern) ) {
        var p=text.split(begPattern);
        for(i=1;i<p.length;i++) {
            var n=p[i].split(endPattern);
            foundCode=foundCode+n[0]+';\n';
        }
        return foundCode;
    }
    return foundCode;
}
function getTagValue(tag)
{
    return document.getElementById(tag).value;
}
function ClearTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.innerHTML="";
    }
}
function EFormAction(action) {
    var arr = new Array();
    arr[0] = 'dataset';
    arr[1] = 'eventset';
    arr[2] = 'submit';
    arr[3] = 'reset';
    arr[4] = 'email';
    for(var i=0; i < arr.length; i++) {
        var tag = 'eform_'+arr[i];
        var id = document.getElementById(tag);
        id.disabled=action;
    }
}
function HideTag(tag) {
  var id=document.getElementById(tag);
  if (id) {
      id.className="hide";
  }
}
function ShowTag(tag) {
  var id=document.getElementById(tag);
  if (id) {
      id.className="show";
  }
}
function SetMethod(tag)
{
    var id=document.getElementById('method');
    id.value=tag;
}
function ajaxAction(user) {
    var lfn=document.getElementById('lfn').value;
    var method=document.getElementById('method').value;
    if (method=='request') {
     ajaxRequest(user,lfn);
    }  else if (method=='status') {
     ajaxStatus(user,lfn);
    }  else if (method=='cancel') {
     ajaxCancel(user,lfn);
    }  else {
     alert('ERROR');
    }
}
function ajaxRequest(user,lfn)
{
    wait();
    ajaxEngine.sendRequest('ajaxRequest','user='+user,'lfn='+lfn);
    setTimeout('ajaxStatusOne(\''+user+'\',\''+lfn+'\')',3000);
}
function ajaxCancel(user,lfn)
{
    wait();
    ajaxEngine.sendRequest('ajaxCancel','user='+user,'lfn='+lfn);
}
function ajaxStatus(user,lfn)
{
    wait();
    ajaxEngine.sendRequest('ajaxStatus','lfn='+lfn,'user='+user);
}
function ajaxStatusOne(user,lfn)
{
    ajaxEngine.sendRequest('ajaxStatusOne','user='+user,'lfn='+lfn);
}
function ajaxdbsStatus(dbs)
{
    var id=document.getElementById('_response').innerHTML;
    if (id && id.search('loading.gif')) {
        ajaxEngine.sendRequest('ajaxdbsStatus','dbs='+dbs);
    }
}
function ajaxcmsRunStatus(user, requestid)
{
    ajaxEngine.sendRequest('ajaxcmsRunStatus','user='+user,'requestid='+requestid);
}
function ajaxRemove(user,lfn)
{
    ajaxEngine.sendRequest('ajaxRemove','lfn='+lfn,'user='+user);
}
function ajaxrm(user, lfn)
{
    ajaxEngine.sendRequest('ajaxRemove','lfn='+lfn,'user='+user,'remove=1');
    ClearTag('_response');
}
function ajaxResolveLfn(user)
{
    wait();
    ajaxdbsStatus('cms_dbs_prod_global');
    var dataset=document.getElementById('dataset').value;
    var run=document.getElementById('run').value;
    var minEvt=document.getElementById('minEvt').value;
    var maxEvt=document.getElementById('maxEvt').value;
    var branch=document.getElementById('branch').value;
    ajaxEngine.sendRequest('ajaxResolveLfn','user='+user,'dataset='+dataset,'run='+run,'minEvt='+minEvt,'maxEvt='+maxEvt,'branch='+branch);
}
function ajaxGetEvent(user)
{
    wait();
    var dataset=document.getElementById('eform_dataset').value;
    var evtset=document.getElementById('eform_eventset').value;
    var email=document.getElementById('eform_email').value;
    ajaxEngine.sendRequest('ajaxGetEvent','user='+user,'dataset='+dataset,'eventset='+evtset,'email='+email);
}
function ajaxUnlock()
{
    ajaxEngine.sendRequest('ajaxUnlock');
}
ajaxEngine.registerRequest('ajaxUnlock','unlock');

ajaxEngine.registerAjaxElement('lfnsHolder');
ajaxEngine.registerRequest('ajaxResolveLfn','resolveLfn');
ajaxEngine.registerRequest('ajaxGetEvent','getEvent');
ajaxEngine.registerRequest('ajaxRequest','request');
ajaxEngine.registerRequest('ajaxCancel','cancel');
ajaxEngine.registerRequest('ajaxStatus','status');
ajaxEngine.registerRequest('ajaxRemove','remove');
ajaxEngine.registerRequest('ajaxStatusOne','statusOne');
ajaxEngine.registerRequest('ajaxdbsStatus','dbsStatus');
ajaxEngine.registerRequest('ajaxcmsRunStatus','cmsRunStatus');

//var ajaxUpdater = new Updater('_response');
var ajaxUpdater = new dbsUpdater('_response');
ajaxEngine.registerAjaxObject('_response',ajaxUpdater);
//ajaxEngine.registerAjaxElement('_response');
function wait() {
    var id=document.getElementById('_response');
    id.innerHTML='<div><img src="images/loading.gif" alt="loading" /> please wait</div>';
}
function switchForms(tag1, tag2) {
    id1 = document.getElementById('id_'+tag1);
    id2 = document.getElementById('id_'+tag2);
    id1.className='highlight';
    id2.className='box';

    f1 = document.getElementById('form_'+tag1);
    f2 = document.getElementById('form_'+tag2);
    f1.className="show";
    f2.className="hide";
    if (tag1 == 'files') {
        var myUrl=window.location.href;
        load(myUrl);
    } else {
        ClearTag('_response');
    }
}
function load(url) {
    window.location.href=url;
}

