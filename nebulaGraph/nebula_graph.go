package nebulaGraph

import (
	"fmt"
	nebula "github.com/vesoft-inc/nebula-go"
	"time"
)

type NebulaConf struct {
	Address         string
	Port            int
	UserName        string
	Password        string
	TimeOut         int
	IdleTime        int
	MaxConnPoolSize int
	MinConnPoolSize int
}

var NebulaLog = nebula.DefaultLogger{}

type NebulaObj struct {
	pool *nebula.ConnectionPool
	conf *NebulaConf
}

func NewGraphInstance(conf *NebulaConf) (*NebulaObj, error) {
	hostAdress := nebula.HostAddress{Host: conf.Address, Port: conf.Port}
	hostList := make([]nebula.HostAddress, 0, 1)
	hostList = append(hostList, hostAdress)
	poolConf := nebula.PoolConfig{
		TimeOut:  time.Duration(conf.TimeOut) * time.Millisecond,
		IdleTime: time.Duration(conf.IdleTime) * time.Millisecond,
		MaxConnPoolSize: func() int {
			if conf.MaxConnPoolSize < 10 {
				return 10
			} else {
				return conf.MaxConnPoolSize
			}
		}(),
		MinConnPoolSize: func() int {
			if conf.MinConnPoolSize < 1 {
				return 1
			} else {
				return conf.MaxConnPoolSize
			}
		}(),
	}
	pool, err := nebula.NewConnectionPool(hostList, poolConf, NebulaLog)
	if err != nil {
		return nil, err
	}

	return &NebulaObj{
		pool: pool,
		conf: conf,
	}, nil
}

//获取一个session
func (m *NebulaObj) getSession() (*nebula.Session, error) {
	return m.pool.GetSession(m.conf.UserName, m.conf.Password)
}

//检查返回结果是否成功
func (m *NebulaObj) checkResultSet(res *nebula.ResultSet) error {
	if !res.IsSucceed() {
		return fmt.Errorf("ErrorCode: %v, ErrorMsg: %s", res.GetErrorCode(), res.GetErrorMsg())
	}
	return nil
}

//直接运行
func (m *NebulaObj) Execute(dl string) (*nebula.ResultSet, error) {
	return m.overExecute("", func(session *nebula.Session) (*nebula.ResultSet, error) {
		return session.Execute(dl)
	})
}

//包装运行
func (m *NebulaObj) overExecute(spaceName string, f func(session *nebula.Session) (*nebula.ResultSet, error)) (*nebula.ResultSet, error) {
	session, err := m.getSession()
	if err != nil {
		return nil, fmt.Errorf("get session err:%s", err.Error())
	}
	defer session.Release()

	if spaceName != "" {
		err := m.useSpace(spaceName, session)
		if err != nil {
			return nil, err
		}
	}
	resultSet, err := f(session)
	if err != nil {
		return nil, err
	}
	if err = m.checkResultSet(resultSet); err != nil {
		return nil, err
	}
	return resultSet, nil
}

//use图空间
func (m *NebulaObj) useSpace(spaceName string, session *nebula.Session) error {
	resultSet, err := session.Execute(fmt.Sprintf("USE %s;", spaceName))
	if err != nil {
		return err
	}
	if err = m.checkResultSet(resultSet); err != nil {
		return err
	}
	return nil
}

//创建图空间
func (m *NebulaObj) CreateSpace(spaceName string, ifNotExists bool, pnum, rnum int, charset, collate string) error {

	if charset == "" {
		charset = "utf8"
	}
	if collate == "" {
		collate = "utf8_bin"
	}
	if pnum == 0 {
		pnum = 100
	}
	if rnum == 0 {
		rnum = 3
	}

	ddl := "CREATE SPACE "
	if ifNotExists {
		ddl += "IF NOT EXISTS "
	}
	ddl += fmt.Sprintf("%s (partition_num = %d, replica_factor = %d, charset = %s, collate = %s);", spaceName, pnum, rnum, charset, collate)
	_, err := m.overExecute("", func(session *nebula.Session) (*nebula.ResultSet, error) {
		return session.Execute(ddl)
	})
	if err != nil {
		return err
	}
	return nil
}

//创建标签
func (m *NebulaObj) CreateTag(spaceName, name string, ifNotExists bool, items map[string]string, options string) error {
	return m.createTagOrEdge("TAG", spaceName, name, ifNotExists, items, options)
}

//创建边类型
func (m *NebulaObj) CreateEdge(spaceName, name string, ifNotExists bool, items map[string]string, options string) error {
	return m.createTagOrEdge("EDGE", spaceName, name, ifNotExists, items, options)
}
func (m *NebulaObj) createTagOrEdge(t, spaceName, name string, ifNotExists bool, items map[string]string, options string) error {
	ddl := fmt.Sprintf("CREATE %s ", t)
	if ifNotExists {
		ddl += "IF NOT EXISTS "
	}
	ddl = fmt.Sprintf("%s %s(", ddl, name)
	itemsLen := len(items)
	if itemsLen != 0 {
		s := ""
		for k, v := range items {
			s += fmt.Sprintf("%s %s,", k, v)
		}

		if string(s[len(s)-1]) == "," {
			s = s[:len(s)-1]
		}
		ddl += fmt.Sprintf("%s)", s)
	}

	if options != "" {
		ddl += options
	}

	ddl += ";"

	_, err := m.overExecute(spaceName, func(session *nebula.Session) (*nebula.ResultSet, error) {
		return session.Execute(ddl)
	})
	if err != nil {
		return err
	}
	return nil
}
