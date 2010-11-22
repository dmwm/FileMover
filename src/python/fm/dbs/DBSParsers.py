#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
DBS parser utils
"""

from xml.dom.minidom import parseString
from fm.utils.Utils import sizeFormat

def parseDBSoutput_DBS_2_0_5(data):
    """
    DBS XML parser for DBS server DBS_2_0_5 and earlier
    """
    dom = parseString(data)
    oList = []
    for node in dom.getElementsByTagName('result'):
        if node.hasAttribute('FILES_FILESIZE') and \
                node.hasAttribute('FILES_LOGICALFILENAME'):
            oList.append((str(node.getAttribute('FILES_LOGICALFILENAME')),
                sizeFormat(node.getAttribute('FILES_FILESIZE')) ))
        elif node.hasAttribute('FILES_LOGICALFILENAME') and \
                node.hasAtribute('APPVERSION_VERSION'):
            oList.append((str(node.getAttribute('FILES_LOGICALFILENAME')),
                node.getAttribute('APPVERSION_VERSION') ))
        elif node.hasAttribute('FILES_LOGICALFILENAME'):
            oList.append(str(node.getAttribute('FILES_LOGICALFILENAME')))
        elif node.hasAttribute('DATATIER_NAME'):
            oList.append(node.getAttribute('DATATIER_NAME'))
        elif node.hasAttribute('BLOCK_NAME') and \
                node.hasAttribute('STORAGEELEMENT_SENAME'):
            oList.append((node.getAttribute('BLOCK_NAME'),
                node.getAttribute('STORAGEELEMENT_SENAME')))
        elif node.hasAttribute('BLOCK_NAME'):
            oList.append(node.getAttribute('BLOCK_NAME'))
        elif node.hasAttribute('APPVERSION_VERSION'):
            oList.append(node.getAttribute('APPVERSION_VERSION'))
    return oList

def parseDBSoutput_DBS_2_0_6(data):
    """
    DBS XML parser for DBS server DBS_2_0_6 and later
    """
    dom  = parseString(data)
    datalist = []
    for node in dom.getElementsByTagName('row'):
        olist = []
        for child in node.childNodes:
            subnode = child.firstChild
            if  not subnode:
                continue
            if  child.nodeName == 'file.size':
                data = sizeFormat(subnode.data)
            else:
                data = subnode.data
            olist.append(data)
        if  len(olist) == 1:
            datalist.append(olist[-1])
        else:
            datalist.append(olist)
    return datalist

