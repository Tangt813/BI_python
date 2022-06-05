import py2neo.data
from flask import Flask, request
from py2neo import Graph

app = Flask(__name__)
graph = Graph("neo4j://150.158.47.16:7687", auth=("neo4j", "123456"))


def getMark(hi, pi, upi, alpha=1, beta=1, gamma=1):
    return (hi / 10) * alpha + (pi / 100) * beta + (upi / 100) * gamma


@app.route("/")
def hello():
    return "Hello World!"


# 端口测试
@app.route("/test")
def test():
    returnObject = {'msg': graph.run("MATCH (n:author{author_name:'Z. Liu'}) RETURN n LIMIT 25").data(),
                    'code': '400'}
    return returnObject


'''
输入一个实体（如某作者 A 或研究主题词 A），查询其关联的所有关系和关联实体
参数：
name:名字，如：Z. Liu
nodeType:类型,如：author
'''


@app.route("/search/searchOneRel")
def searchOneRel():
    name = request.args.get("name")
    nodeType = request.args.get("nodeType")
    nodes = []
    links = []
    if nodeType == 'author':
        res = graph.run(
            "match (a:" + nodeType + "{author_name:\"" + name + "\"})-[rel]->(nei) "
                                                                     "return a,nei,startnode(rel),endnode(rel),type(rel)").data()
    elif nodeType == 'paper':
        res = graph.run(
            "match (p:" + nodeType + "{title:\"" + name + "\"})<-[rel]->(nei) "
                                                          "return t,nei,startnode(rel),endnode(rel),type(rel)").data()
    else:
        res = graph.run(
            "match (t:" + nodeType + "{name:\"" + name + "\"})<-[rel]->(nei) "
                                                         "return t,nei,startnode(rel),endnode(rel),type(rel)").data()
    for linkDict in res:
        link = {}
        cnt = 0
        for node in linkDict:
            cnt += 1
            if cnt == 3:
                link['from'] = linkDict[node].identity
            elif cnt == 4:
                link['to'] = linkDict[node].identity
            elif cnt == 5:
                link['text'] = linkDict[node]
            else:
                identity = linkDict[node].identity
                nodeDict = dict(linkDict[node])
                isIn = False
                for inNode in nodes:
                    if inNode['id'] == identity:
                        isIn = True
                        break
                if not isIn:
                    nodeDict['id'] = identity
                    if 'name' not in nodeDict.keys():
                        if 'title' in nodeDict.keys():
                            nodeDict['name']=nodeDict['title']
                        elif 'author_name' in nodeDict.keys():
                            nodeDict['name']=nodeDict['author_name']
                    if 'author_id' in nodeDict.keys():
                        nodeDict['color']="#4169E1"
                    elif 'paper_id' in nodeDict.keys():
                        nodeDict['color']="#FFC0CB"
                    elif 'institute_id'in nodeDict.keys():
                        nodeDict['color']="#7FFF00"
                    elif 'publisher_id'in nodeDict.keys():
                        nodeDict['color']="#FFA500"
                    elif 'interest_id'in nodeDict.keys():
                        nodeDict['color']="#D3D3D3"
                    nodes.append(nodeDict)
        link['color'] = "#000000"
        links.append(link)
    msg = {'nodes': nodes, 'links': links}
    returnObject = {
        'msg': msg,
        'code': '400'}
    return returnObject


'''
输入两个实体（如作者 A 和作者 B），查询其可能存在的多跳关系
参数：
name1:名字1
name2:名字2
nodeType1：类型1
nodeType2：类型2
'''


@app.route("/search/searchTwoRel")
def searchTwoRel():
    name1 = request.args.get("name1")
    name2 = request.args.get("name2")
    nodeType1 = request.args.get("nodeType1")
    nodeType2 = request.args.get("nodeType2")
    neo4jName1 = ""
    neo4jName2 = ""
    if nodeType1 == 'author':
        neo4jName1 = 'author_name'
    elif nodeType1 == 'paper':
        neo4jName1 = 'title'
    elif nodeType1 == 'publisher' or nodeType1 == 'interest' or nodeType1 == 'institute':
        neo4jName1 = 'name'

    if nodeType2 == 'author':
        neo4jName2 = 'author_name'
    elif nodeType2 == 'paper':
        neo4jName2 = 'title'
    elif nodeType2 == 'publisher' or nodeType2 == 'interest' or nodeType2 == 'institute':
        neo4jName2 = 'name'
    print(
        "MATCH p=shortestpath((n1:" + nodeType1 + "{" + neo4jName1 + ":\"" + name1 + "\"})<-[*..4]->(n2:" + nodeType2 + "{" + neo4jName2 + ":\"" + name2 + "\"})) RETURN p")
    res = graph.run(
        "MATCH p=shortestpath((n1:" + nodeType1 + "{" + neo4jName1 + ":\"" + name1 + "\"})<-[*..4]->(n2:" + nodeType2 + "{" + neo4jName2 + ":\"" + name2 + "\"})) RETURN p ").data()
    nodes = []
    links = []
    for i in range(len(res)):
        for node in res[i]['p'].nodes:
            identity = node.identity
            isIn = False
            nodeDict = dict(node)
            for resNode in nodes:
                if resNode['id'] == identity:
                    isIn = True
                    break
            if not isIn:
                nodeDict['id'] = identity
                if 'name' not in nodeDict.keys():
                    if 'title' in nodeDict.keys():
                        nodeDict['name'] = nodeDict['title']
                    elif 'author_name' in nodeDict.keys():
                        nodeDict['name'] = nodeDict['author_name']
                if 'author_id' in nodeDict.keys():
                    nodeDict['color'] = "#4169E1"
                elif 'paper_id' in nodeDict.keys():
                    nodeDict['color'] = "#FFC0CB"
                elif 'institute_id' in nodeDict.keys():
                    nodeDict['color'] = "#7FFF00"
                elif 'publisher_id' in nodeDict.keys():
                    nodeDict['color'] = "#FFA500"
                elif 'interest_id' in nodeDict.keys():
                    nodeDict['color'] = "#D3D3D3"
                nodes.append(nodeDict)

        for j in range(len(res[i]['p'].relationships)):
            link = {}
            link['from'] = res[i]['p'].nodes[j].identity
            link['to'] = res[i]['p'].nodes[j + 1].identity
            link['text'] = res[i]['p'].relationships[j].__class__.__name__
            link['color']="#000000"
            links.append(link)

    msg = {'nodes': nodes, 'links': links}
    returnObject = {
        'msg': msg,
        'code': '400'}
    return returnObject


'''
查询在某个领域中的关键作者和单位是什么，为什么？
参数：
field:领域名字 如：novel approach
alpha：h-index权值
beta:p-index权值
gamma:up-index权值
注：返回是由高到低排好序的2个列表，分别是作者列表和单位列表
原因：按照作者的3个index加权取值，关键作者所在的单位就是关键单位
'''


@app.route("/search/importantAuthor")
def importantAuthor():
    field = request.args.get("field")
    author = graph.run("match (i:interest{name:\"" + field + "\"})<-->(a:author) return a")
    authorData = author.data()
    authorResult = []
    institutionSet = set()
    mark = 0.3
    alpha=request.args.get("alpha")
    beta= request.args.get("beta")
    gamma=request.args.get("gamma")

    for data in authorData:
        thisMark = getMark(data['a']["hi"], data['a']["pi"], data['a']["upi"], int(alpha),int(beta), int(gamma))

        if thisMark > mark:
            data['a']['mark'] = thisMark
            authorResult.append(data['a'])
            if len(authorResult) > 20:
                mark = thisMark
            institutionSet.add(data['a']['author_id'])
    authorResult.sort(key=lambda author: author["mark"])
    authorResult.reverse()
    institutionList = []
    for institutionData in institutionSet:
        institution = graph.run(
            "match (a:author{author_id:" + str(institutionData) + "})-[r:workIn]->(i:institute) return i").data()
        institutionList.append(institution)
    msg = {'authorResult': authorResult, 'institutionSet': institutionList}
    returnObject = {
        'msg': msg,
        'code': '400'}
    return returnObject


'''
查询在某个领域中的关键期刊/会议是什么，为什么？
参数：
field：领域名字。 如：novel approach
注：返回的是关键会议期刊的排好序的列表
原因：根据该领域中作者发布到某个会议的数量来评价会议的重要性。
'''


@app.route("/search/importantVenue")
def importantVenue():
    field = request.args.get("field")
    authorData = graph.run(
        "match (i:interest{name:\"" + field + "\"})<-[r:interestedIn]-(a:author) return distinct a").data()
    # 找出所有该领域的作者
    venueDir = {}
    venueDataDict = {}
    for data in authorData:
        if data['a']['write_count'] <= 40:
            continue

        publisher = graph.run("match (a:author{author_id:" + str(data["a"][
                                                                     'author_id']) + "})-[r:write]->(p:paper)-[publish:publishedBy]->(per:publisher) return distinct per").data()
        # 找出所有该领域的作者写的所有paper
        for publisherData in publisher:  # 获取到每个paper
            # 获取每个paper发表的期刊
            if publisherData["per"]['publisher_id'] in venueDir.keys():
                venueDir[publisherData["per"]['publisher_id']] = venueDir[publisherData["per"]['publisher_id']] + 1
            else:
                venueDir[publisherData["per"]['publisher_id']] = 1
                venueDataDict[publisherData["per"]['publisher_id']] = publisherData["per"]

    resultList = []
    for times in venueDir:
        if venueDir[times] > 10:
            venueDataDict[times]['mark'] = venueDir[times]
            resultList.append(venueDataDict[times])
    resultList.sort(key=lambda data: data["mark"])
    resultList.reverse()
    returnObject = {
        'msg': resultList,
        'code': '400'}
    return returnObject


if __name__ == "__main__":
    app.run(host='localhost', port=8084)
