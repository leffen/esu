package esu

import "github.com/mohae/deepcopy"

type jsonMap map[string]interface{}

func (m jsonMap) copy() jsonMap {
	return deepcopy.Iface(m).(jsonMap)
}

func (m jsonMap) merge(other jsonMap) jsonMap {
	out := jsonMap{}
	for k, v := range m {
		out[k] = v
	}
	for k, v := range other {
		if v != nil {
			out[k] = v
		} else {
			delete(out, k)
		}
	}
	return out
}
