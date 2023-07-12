package main

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func NewIndexerInformer(
	lw cache.ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h cache.ResourceEventHandler,
	indexers cache.Indexers,
) (cache.Indexer, cache.Controller) {
	// This will hold the client state, as we know it.
	clientState := NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, indexers)

	return clientState, newInformer(lw, objType, resyncPeriod, h, clientState)
}

func newInformer(
	lw cache.ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h cache.ResourceEventHandler,
	clientState cache.Store,
) cache.Controller {
	// This will hold incoming changes. Note how we pass clientState in as a
	// KeyLister, that way resync operations will result in the correct set
	// of update/delete deltas.
	fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
		KnownObjects:          clientState,
		EmitDeltaTypeReplaced: true,
	})

	cfg := &cache.Config{
		Queue:            fifo,
		ListerWatcher:    lw,
		ObjectType:       objType,
		FullResyncPeriod: resyncPeriod,
		RetryOnError:     false,

		Process: func(obj interface{}) error {
			// from oldest to newest
			for _, d := range obj.(cache.Deltas) {
				metaObj, err := meta.Accessor(d)
				if err != nil {
					return cache.KeyError{Obj: d, Err: err}
				}
				metaData := meta.AsPartialObjectMetadata(metaObj)

				switch d.Type {
				case cache.Sync, cache.Replaced, cache.Added, cache.Updated:
					// fmt.Printf("add obj: %v\n", metaData)ß
					if old, exists, err := clientState.Get(metaData); err == nil && exists {
						if err := clientState.Update(metaData); err != nil {
							return err
						}
						h.OnUpdate(old, metaData)
					} else {
						if err := clientState.Add(metaData); err != nil {
							return err
						}
						h.OnAdd(metaData)
					}
				case cache.Deleted:
					if err := clientState.Delete(metaData); err != nil {
						return err
					}
					h.OnDelete(metaData)
				}
			}
			return nil
		},
	}
	return cache.New(cfg)
}

// Multiplexes updates in the form of a list of Deltas into a Store, and informs
// a given handler of events OnUpdate, OnAdd, OnDelete
func processDeltas(
	// Object which receives event notifications from the given deltas
	handler cache.ResourceEventHandler,
	clientState cache.Store,
	deltas cache.Deltas,
	isInInitialList bool,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.Object

		switch d.Type {
		case cache.Sync, cache.Replaced, cache.Added, cache.Updated:
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				handler.OnUpdate(old, obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				handler.OnAdd(obj)
			}
		case cache.Deleted:
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			handler.OnDelete(obj)
		}
	}
	return nil
}
