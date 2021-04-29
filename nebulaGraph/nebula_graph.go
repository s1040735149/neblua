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

func (m *NebulaObj) getSession() (*nebula.Session, error) {
	return m.pool.GetSession(m.conf.UserName, m.conf.Password)
}
func (m *NebulaObj) checkResultSet(res *nebula.ResultSet) error {
	if !res.IsSucceed() {
		return fmt.Errorf("ErrorCode: %v, ErrorMsg: %s", res.GetErrorCode(), res.GetErrorMsg())
	}
	return nil
}

func (m *NebulaObj) CreateSpace(spaceName string, exists bool, pnum, rnum int, charset, collate string) error {

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

	session, err := m.getSession()
	if err != nil {
		return fmt.Errorf("get session err:%s", err.Error())
	}
	defer session.Release()

	ddl := "CREATE SPACE "
	if exists {
		ddl += "IF NOT EXISTS "
	}
	ddl += fmt.Sprintf("%s (partition_num = %d, replica_factor = %d, charset = %s, collate = %s);", spaceName, pnum, rnum, charset, collate)
	resultSet, err := session.Execute(ddl)
	if err != nil {
		return err
	}
	if err = m.checkResultSet(resultSet); err != nil {
		return err
	}
	return nil
}
