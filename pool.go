package ratelimit

type pool struct {
	buffer chan *Bucket
}

func newPool(count int) *pool {

	ret := new(pool)

	ret.buffer = make(chan *Bucket, count)
	return ret
}

func (p *pool) Get() *Bucket {
	select {
	case ret := <-p.buffer:
		return ret
	default:
	}
	return nil
}

func (p *pool) Pool(u *Bucket) {
	select {
	case p.buffer <- u:
	default:
	}

}
