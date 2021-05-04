package mqneo4j

// Neo4j v1.0.1
import (
	"bytes"
	"fmt"
	"log"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

/*
Connection between queue managers
*/
type Connection struct {
	ID          int64  `json:"id"`
	ChannelName string `json:"channelName"`
	ChannelType int    `json:"channelType"`
	StartID     int64  `json:"startId"`
	EndID       int64  `json:"endId"`
}

/*
QueueManager ...
*/
type QueueManager struct {
	ID        int64  `json:"id"`      // Neo4j unique id
	QmgrID    int64  `json:"qmgrid"`  // Qmgr unique id from data extract
	Name      string `json:"name"`    // Name of queue manager
	Repos     int    `json:"repos"`   // Queue manager repos type - Full, Partial, Normal
	ToolTip   string `json:"tooltip"` // Not needed
	Layer     int    `json:"layer"`   // Layer
	Decom     int64  `json:"decom"`   // Decomissioned ? 0 - no, 9 - yes
	Host      string `json:"host"`    // Host name
	Host2     string `json:"host2"`   // Second host for multi instance
	MultiInst int64  `json:"multi"`   // Multi instance
}

/*
Application ...
*/
type Application struct {
	ID      int64  `json:"id"`      // Neo4j unique id
	AppID   int64  `json:"appid"`   // Application unique id from data extract
	Name    string `json:"name"`    // Name of the application
	Owner   string `json:"owner"`   // Owner of the application
	ToolTip string `json:"tooltip"` // Tooltip
	Layer   int    `json:"layer"`   // Layer

}

/*
Neo4jAPIResponse ...
*/
type Neo4jAPIResponse struct {
	QueueManagers []QueueManager `json:"queuemanagers"`
	Applications  []Application  `json:"applications"`
	Connections   []Connection   `json:"connections"`
}

/*
ErrorResponse ...
*/
type ErrorResponse struct {
	Code           string `json:"code"`
	Info           string `json:"Info"`
	Msg            string `json:"Message"`
	HTTPStatusCode int    `json:"HttpStatusCode"`
	HTTPStatus     string `json:"HttpStatus"`
	Page           string `json:"Page"`
}

/*
Match ...
*/
type Match struct {
	Neo4jQuery          string
	Layer               int
	IncludeNodes        bool
	IncludeApplications bool
	Username            string
}

var (
	buf    bytes.Buffer
	logger = log.New(&buf, "logger: ", log.Lshortfile+log.Ldate+log.Ltime)
)

/*
ConnectToNeo4j ...
*/
func ConnectToNeo4j(uri, username, password string) (neo4j.Driver, error) {
	log.Print("Neo4j init ")

	driver, err := neo4j.NewDriver(uri,
		neo4j.BasicAuth(username, password, ""),
		func(c *neo4j.Config) { c.Encrypted = false })

	if err != nil {
		return nil, err
	}
	log.Print("Neo4j init done")
	return driver, nil
}

func closeSession(session neo4j.Session) {

	log.Print("Closing neo4j session")
	session.Close()

}

/*
RunQuery function
Query the Neo4j database with the query string in the Match struct
*/
func (m *Match) RunQuery(driver neo4j.Driver) (Neo4jAPIResponse, error) {

	//fmt.Println("Running query in RunQuery")
	//http://doc.we-yun.com:1008/neo4j-doc/others/neo4j-driver-manual-1.7-go.pdf
	//result, err := session.Run("MATCH p=(q:QueueManager {name: $name})--() RETURN p LIMIT 25", map[string]interface{}{"name": name})

	// Resp hold the response nessages
	resp := Neo4jAPIResponse{}

	// Create a new Neo4j session
	session, err := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer closeSession(session)
	if err != nil {
		return resp, err
	}

	//fmt.Println("Session created")

	// Run the query ...
	respqmgr := make(map[int64]QueueManager)
	respapp := make(map[int64]Application)
	respconns := make(map[int64]Connection)
	maxLayers := m.Layer
	m.Layer = 1

	// Execute the main query ....
	//fmt.Printf("Query: %s\n", m.Neo4jQuery)
	resp, err = m.runQueryOnCurrentNode(session, resp, respqmgr, respapp, respconns, false)
	if err != nil {
		fmt.Println("Error retruned from runQueryOnCurrentNode")
		return resp, err
	}

	// Have we asked for recursive calls ...
	if maxLayers > m.Layer {
		for _, node := range respqmgr {

			s := fmt.Sprintf("match p=(q {qmid: %d})-[]-() return p", node.QmgrID)
			m.Neo4jQuery = s
			m.Layer++
			sub, err := m.runQueryOnCurrentNode(session, resp, respqmgr, respapp, respconns, m.IncludeNodes)
			if err != nil {
				//fmt.Println("Main : " + sub.QueueManagers[0].Name)
			}
			resp = sub

		}
	} else {
		//fmt.Println("No additional layers added, Max layers reached")
	}

	return resp, err

}

/*
execute query
*/
func (m *Match) executeNeo4jQuery(session neo4j.Session) (neo4j.Result, error) {
	result, err := session.Run(m.Neo4jQuery, nil)
	return result, err

} // End of executeNeo4jQuery

/*
run the query and process the results
*/
func (m *Match) runQueryOnCurrentNode(session neo4j.Session, resp Neo4jAPIResponse,
	respqmgr map[int64]QueueManager, respapp map[int64]Application,
	respconns map[int64]Connection,
	onlyRelationships bool) (Neo4jAPIResponse, error) {

	result, err := m.executeNeo4jQuery(session)
	if err != nil {
		//fmt.Printf("%s\n", err)
		return resp, err
	}

	var nodes []neo4j.Node

	// For each of the returned results, loop through and extract the nodes and relationships
	for result.Next() {

		var relationships []neo4j.Relationship

		values := result.Record().Values()
		path, ok := values[0].(neo4j.Path)

		// When the query string contains a "where" clause, then relationships dont seem to be returned
		// ... OK seems to be false, so extract the nodes
		if !ok {
			nodes = append(nodes, values[0].(neo4j.Node))

		} else {
			nodes = path.Nodes()
			relationships = path.Relationships()
		}

		// Have we request only the relationships ?
		if !onlyRelationships {
			var nodecount int
			for _, node := range nodes {

				nodecount++
				lab := node.Labels()
				//fmt.Println("Label:", lab)

				switch lab[0] { // There probably can be more ... but for now, we are only going to check the 1st
				case "QueueManager": // Label is 'QueueManager' - this is from Neo4j nodes
					processQueueManagerNode(&resp, respqmgr, node, m)
					break
				case "Application": // Label is 'Applications'
					processApplicationNode(&resp, respapp, node, m)
					break
				default:
					//fmt.Println("Unmanaged node type:", lab)
				}
			}
		}

		//nodeCount := len(respqmgr)
		//relCount := len(relationships)

		//fmt.Printf("nodeCount is %d\n", nodeCount)
		//fmt.Printf("relCount  is %d\n", relCount)

		// for MQ PCF values, see https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.1.0/com.ibm.mq.javadoc.doc/WMQJavaClasses/constant-values.html#com.ibm.mq.constants.CMQC.MQOT_SVRCONN_CHANNEL

		// For each relationship ...
		for _, rels := range relationships {

			rel := rels.(neo4j.Relationship)
			//	fmt.Printf("Id : %d, StartId : %d, EndId: %d, Type %s\n", rel.Id(), rel.StartId(), rel.EndId(), rel.Type())

			// Set the channel type based on the name of the relationship
			// ... this will change to unclude a numeric value, base on the MQ channel type value
			channelType := channelType(rel.Type())
			conns := Connection{
				ID:          rel.Id(),
				ChannelName: rel.Type(),  // Type is the name of the channel
				ChannelType: channelType, // MQ channel type
				StartID:     rel.StartId(),
				EndID:       rel.EndId()} // connection object

			if _, value := respconns[rel.Id()]; !value {
				respconns[rel.Id()] = conns
				resp.Connections = append(resp.Connections, conns) // add the queue manager to the response queue managers

			} else {
				//fmt.Printf("ID %d already added\n", rel.Id())
			}

		} // End of relationship loop

	} // End of results loop

	return resp, nil
} // End of runQueryOnCurrentNode

// Channel Type
func channelType(channelName string) int {

	var channelType int

	switch channelName {

	case "SENDER":
		channelType = 1
		break
	case "SERVER":
		channelType = 2
		break
	case "RECEIVER":
		channelType = 3
		break
	case "REQUESTER":
		channelType = 4
		break
	case "CLIENT_CONN":
		channelType = 6
		break
	case "CLNTCONN":
		channelType = 6
		break
	case "SERVER_CONN":
		channelType = 7
		break
	case "SVRCONN":
		channelType = 7
		break
	case "CLUSTER_RECEIVER":
		channelType = 8
		break
	case "CLUSRCVR":
		channelType = 8
		break
	case "CLUSTER_SENDER":
		channelType = 9
		break
	case "CLUSSDR":
		channelType = 9
		break
	case "MQTT":
		channelType = 10
		break

	default:
		channelType = 99
		break
	}

	return channelType

} // End of channelType

/*
Process Queue Manager nodes
*/
func processQueueManagerNode(resp *Neo4jAPIResponse, respqmgr map[int64]QueueManager, node neo4j.Node, m *Match) {

	mask := "id:%d, qmid:%d, name:%s, type:%s, server:%s" // mask for tooltip

	nodeprops := node.Props()
	nodeid := node.Id()
	qmid, ok := nodeprops["qmid"]
	if !ok {
		panic("Error getting mqid property")
	}
	name, ok := nodeprops["name"]
	if !ok {
		panic("Error getting name property")
	}
	host, ok := nodeprops["host"]
	if !ok {
		panic("Error getting host property")
	}
	host2, ok := nodeprops["host2"]
	if !ok {
		host2 = ""
	}
	repos, ok := nodeprops["type"]
	if !ok {
		repos = "Partial"
	}
	multi, ok := nodeprops["multi"]
	if !ok {
		multi = 0
	}

	// Queue Manager type
	var qmtype = 0 // Full
	switch repos {
	case "Full":
		qmtype = 0 // Full repos
		break
	case "Partial":
		qmtype = 1 // Partial
		break
	case "Normal":
		qmtype = 2 // Normal
		break
	default:
		qmtype = 3 // Unknown
		break
	}

	//
	decom, ok := nodeprops["decom"].(int64)
	if !ok {
		qmtype = 3
		decom = 0
	}

	var tooltip = fmt.Sprintf(mask, nodeid, qmid.(int64), name.(string), "Full-Partial", host.(string)) // tooltip
	qm := QueueManager{ID: nodeid,
		QmgrID:    qmid.(int64),
		Name:      name.(string),
		Repos:     qmtype,
		ToolTip:   tooltip,
		Layer:     m.Layer,
		Decom:     decom,
		Host:      host.(string),
		Host2:     host2.(string),
		MultiInst: multi.(int64),
	} // queue manager object

	if _, value := respqmgr[nodeid]; !value {
		respqmgr[nodeid] = qm
		resp.QueueManagers = append(resp.QueueManagers, qm) // add the queue manager to the response queue managers
	}

} // End of processQueueManagerNode

// Pprocess application nodes
func processApplicationNode(resp *Neo4jAPIResponse, respapp map[int64]Application, node neo4j.Node, m *Match) {

	mask := "id:%d, qmid:%d, name:%s, type:%s, server:%s" // mask for tooltip

	nodeprops := node.Props()
	nodeid := node.Id()
	appid, ok := nodeprops["id"]
	if !ok {
		panic("Error getting id property")
	}
	name, ok := nodeprops["name"]
	if !ok {
		panic("Error getting name property")
	}
	owner, ok := nodeprops["owner"]
	if !ok {
		panic("Error getting owner property")
	}

	var tooltip = fmt.Sprintf(mask, nodeid, appid.(int64), name.(string), owner.(string)) // tooltip
	app := Application{ID: nodeid,
		AppID:   appid.(int64),
		Name:    name.(string),
		Owner:   owner.(string),
		ToolTip: tooltip,
		Layer:   m.Layer,
	} // application node object

	if _, value := respapp[nodeid]; !value {
		respapp[nodeid] = app
		resp.Applications = append(resp.Applications, app) // Add applications
	}

} // processApplicationNode

// https://medium.com/neo4j/neo4j-go-driver-is-out-fbb4ba5b3a30

// Hello function
func Hello() string {
	return "Hello, from Neo4j driver"
} // End of Hello
