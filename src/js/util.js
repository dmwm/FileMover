function loadMasthead() {
    try {
        insertMastHead('filemover','')
    } catch(err) {
//        txt  = "There was an error during masthead loading.\n\n";
//        txt += "Error description: " + err.description + "\n\n";
//        txt += "Click OK to continue.\n\n";
//        alert(txt);
    }
}
function footerMenuText(){
    return [
{label: "PhEDEx Home", link: "/phedex/", title: "Data placement, transfer monitoring"},
{label: "FileMover", link: "/filemover/", title: "Fetch your favorite LFN"},
    ]
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
function ajaxRequest()
{
    wait();
    var interval = 3000;
    String.prototype.trim = function() { return this.replace(/^\s+|\s+$/g, ""); }
    var file = escape($F('lfn').trim());
    new Ajax.Updater('fm_response', '/filemover/request', 
    { method: 'get' ,
      parameters: 'lfn=' + file,
      onSuccess : setTimeout('ajaxStatusOne("'+file+'","'+interval+'")', interval)
    });
}
function lfn_tag(lfn) {
    var res  = lfn.split("/");
    var last = res[res.length-1];
    return last.replace(".root", "");
}
function ajaxCancel(lfn)
{
    new Ajax.Updater(lfn_tag(lfn), '/filemover/cancel', 
    { method: 'get', parameters: 'lfn=' + lfn });
}
function ajaxRemove(lfn)
{
    new Ajax.Updater(lfn_tag(lfn), '/filemover/remove', 
    { method: 'get', parameters: 'lfn=' + lfn });
}
function ajaxStatusOne(lfn, interval) {
    var limit = 10000; // in miliseconds
    var wait  = parseInt(interval);
    if (wait*2 < limit) {
        wait  = wait*2;
    } else { wait = limit; }
    var tag  = lfn_tag(lfn);
    new Ajax.Updater(tag, '/filemover/statusOne', 
    { method: 'get' ,
      parameters: 'lfn=' + lfn,
      onException: function() {return;},
      onSuccess : function(transport) {
        // look at transport body and match its content,
        // if check_pid still working on request, call again
        if (transport.responseText.match(/<!-- templateLoading -->/)) {
            setTimeout('ajaxStatusOne("'+lfn+'","'+wait+'")', wait);
        }
      }
    });
}
function ajaxResolveLfn()
{
    wait();
    new Ajax.Updater('fm_response', '/filemover/resolveLfn', 
    { method: 'get', parameters: $('resolve_lfn').serialize(true) });
}

function wait() {
    var id=document.getElementById('fm_response');
    id.innerHTML='<div><img src="images/loading.gif" alt="loading" /> please wait</div>';
}
function load(url) {
    window.location.href=url;
}

