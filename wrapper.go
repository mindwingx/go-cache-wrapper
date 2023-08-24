package cachewrapper

import (
	"context"
	"encoding/json"
	"fmt"
	redisCore "github.com/go-redis/redis/v8"
	"github.com/mindwingx/abstraction"
	"github.com/mindwingx/go-helper"
	"time"
)

type (
	redis struct {
		config cacheConfig
		locale abstraction.Locale
		redis  *redisCore.Client
	}

	cacheConfig struct {
		Host     string
		Port     string
		Password string
		Db       int
		Timeout  time.Duration
	}
)

func New(registry abstraction.Registry, locale abstraction.Locale) abstraction.Cache {
	caching := new(redis)
	err := registry.Parse(&caching.config)
	if err != nil {
		helper.CustomPanic("", err)
	}
	caching.locale = locale

	caching.redis = redisCore.NewClient(&redisCore.Options{
		Addr:     fmt.Sprintf("%s:%s", caching.config.Host, caching.config.Port),
		Password: caching.config.Password,
		DB:       caching.config.Db,
	})

	return caching
}

func (r *redis) InitCache() {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.Timeout)
	// call the cancel variable of the above context
	defer cancel()

	_, errConnect := r.redis.Ping(ctx).Result()
	if errConnect != nil {
		helper.CustomPanic(r.locale.Get("cache_conn_err"), errConnect)
	}
}

func (r *redis) Store(key string, data interface{}, duration time.Duration) (err error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if err = r.redis.Set(context.Background(), key, bytes, duration).Err(); err != nil {
		return err
	}

	return nil
}

func (r *redis) Exists(key string) bool {
	return r.redis.Exists(context.Background(), key).Val() == 1
}

func (r *redis) Get(key string) (b []byte, err error) {
	b, err = r.redis.Get(context.Background(), key).Bytes()
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (r *redis) Delete(key string) error {
	return r.redis.Del(context.Background(), key).Err()
}
