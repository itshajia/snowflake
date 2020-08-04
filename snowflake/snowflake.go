package snowflake

import (
	"fmt"
	"sync"
	"time"
)

/**
 * +-------------------------------------------------------------------------------------------------+
 * | 1位符号位(未使用) | 41位时间戳 | 5位机房 |  5位机器  |   12序列号
 * +-------------------------------------------------------------------------------------------------+
 * 1位标识，二进制中最高位为1的都是负数，但是我们生成的id一般都使用整数，所以这个最高位固定是0
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由程序来指定。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69
 * 10位的数据机器位：可以部署在1024个节点，包括5位datacenterId和5位workerId
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号
 *
 **/

const (
	EPOCH = int64(1577808000000) // 纪元、设置起始时间(时间戳/毫秒)：2020-01-01 00:00:00，有效期69年

	BIT_TIMESTAMP  = uint(41) // 时间戳占用位数
	BIT_DATACENTER = uint(5)  // 数据中心占用的位数
	BIT_WORKER     = uint(5)  // 工作节点标识占用的位数
	BIT_SEQUENCE   = uint(12) // 序列号占用的位数

	MAX_TIMESTAMP     = int64(-1 ^ (-1 << BIT_TIMESTAMP))  // 时间戳最大值
	MAX_DATACENTER_ID = int64(-1 ^ (-1 << BIT_DATACENTER)) // 支持的最大数据中心id数量
	MAX_WORKER_ID     = int64(-1 ^ (-1 << BIT_WORKER))     // 支持的最大工作节点id数量
	MAX_SEQUENCE_ID   = int64(-1 ^ (-1 << BIT_SEQUENCE))   // 支持的最大序列id数量
	MAX_BACKWARD_MS   = int64(3)                           // 最大容忍时间，单位毫秒，即如果时钟只是回拨了该变量指定的时间，那么等待相应的时间即可

	SHIFT_TIMESTAMP  = BIT_SEQUENCE + BIT_WORKER + BIT_DATACENTER // 时间戳左移位数
	SHIFT_DATACENTER = BIT_SEQUENCE + BIT_WORKER                  // 机器id左移位数
	SHIFT_WORKER     = BIT_SEQUENCE                               // 工作节点id左移位数
)

type SnowFlake struct {
	sync.Mutex
	epoch        time.Time
	timestamp    int64
	datacenterId int64
	workerId     int64
	sequence     int64
}

func New(datacenterId, machineId int64) (*SnowFlake, error) {
	if datacenterId < 0 || datacenterId > MAX_DATACENTER_ID {
		return nil, fmt.Errorf("datacenterId must be between 0 and %d", MAX_DATACENTER_ID-1)
	}
	if machineId < 0 || machineId > MAX_WORKER_ID {
		return nil, fmt.Errorf("machineId must be between 0 and %d", MAX_WORKER_ID-1)
	}

	now := time.Now() // 获取带有 单调时钟的 time
	// 通过计算，获取带有 单调时钟的 纪元 time
	epoch := now.Add(time.Unix(EPOCH/1000, (EPOCH%1000)*1000000).Sub(now))
	return &SnowFlake{
		epoch:        epoch,
		timestamp:    0,
		datacenterId: datacenterId,
		workerId:     machineId,
		sequence:     0,
	}, nil
}

/**
 * 产生下一个ID
 */
func (s *SnowFlake) NextID() int64 {
	s.Lock()
	defer s.Unlock()
	now := s.GetNow() // 转毫秒

	// 发生时钟回拨
	if now < s.timestamp {
		isOverBackWard := false
		offset := s.timestamp - now
		if offset <= MAX_BACKWARD_MS { // 时钟回拨在可接受范围内，等待即可
			// 时间偏差小于5ms，则等待两倍时间
			time.Sleep(time.Millisecond * time.Duration(offset<<1))

			now = s.GetNow()
			// 如果时间还小于当前时间，则标记为超过时钟回拨范围
			if now < s.timestamp {
				isOverBackWard = true
			}
		} else {
			isOverBackWard = true
		}

		// 如果时钟回拨超出可接受范围，则直接利用扩展字段
		if isOverBackWard == true {
			// 服务时钟被调整，ID生成器停止服务
			panic(fmt.Errorf("Clock moved backwards. Refusing to generate id for %d milliseconds", s.timestamp-now))
		}
	}

	// 如果和最后一次请求处于同一毫秒，则sequence++
	if s.timestamp == now {
		// 当同一时间戳（精度：毫秒）下多次生成id会增加序列号
		s.sequence = (s.sequence + 1) & MAX_SEQUENCE_ID
		if s.sequence == 0 { // 同一毫秒的序列数已经达到最大
			for now <= s.timestamp {
				now = s.GetNow()
			}
		}
	} else {
		// 不同时间戳（精度：毫秒）下直接使用序列号：0
		s.sequence = 0
	}

	// 更新上一次生成ID的时间戳
	s.timestamp = now

	// 进行位移操作，生成int64的唯一ID
	//t := (now - EPOCH) << SHIFT_TIMESTAMP

	return int64((now-EPOCH)<<SHIFT_TIMESTAMP | // 时间戳
		s.datacenterId<<SHIFT_DATACENTER | // 数据中心
		s.workerId<<SHIFT_WORKER | // 机器标识
		s.sequence, // 序列号
	)
}

/**
 * 获取当前时间戳
 */
func (s *SnowFlake) GetNow() int64 {
	return time.Since(s.epoch).Nanoseconds() / 1000000
	// return time.Now().UnixNano() / 1000000
}

/**
 * 获取数据中心ID和机器ID
 */
func GetDeviceID(sid int64) (datacenterId, machineId int64) {
	datacenterId = (sid >> SHIFT_DATACENTER) & MAX_DATACENTER_ID
	machineId = (sid >> SHIFT_WORKER) & MAX_WORKER_ID
	return
}
