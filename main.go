package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-redis/redis/v8"
)

const partitionKey string = "partition"
const mappingKey string = "fsmap:"
const hashKeyNumber int = 500

var ErrInvalidTokenFormat = errors.New("invalid token format")

type FileDetail struct {
	Fid 	 string `json:"fid"`
	FileName string `json:"fileName"`
	FileUrl  string `json:"fileUrl"`
	Size	 int64	`json:"size"`
}

type redisHandler struct {
	client *redis.Client
}

func (h *redisHandler) Get(ctx context.Context, key string) (string, error) {
	strSlice := strings.Split(key, "-")
	if len(strSlice) != 2 {
		return "", ErrInvalidTokenFormat
	}

	masterKey := strSlice[0]
	hashKey := strSlice[1]
	return h.client.HGet(ctx, mappingKey + masterKey, hashKey).Result()
}

func (h *redisHandler) Set(ctx context.Context, key string, value interface{}) error {
	strSlice := strings.Split(key, "-")
	if len(strSlice) != 2 {
		return ErrInvalidTokenFormat
	}
	masterKey := strSlice[0]
	hashKey := strSlice[1]
	_, err := h.client.HSet(ctx, mappingKey + masterKey, hashKey, value).Result()
	return err
}

func (h *redisHandler) Del(ctx context.Context, key string) error {
	strSlice := strings.Split(key, "-")
	if len(strSlice) != 2 {
		return ErrInvalidTokenFormat
	}

	masterKey := strSlice[0]
	hashKey := strSlice[1]
	_, err := h.client.HDel(ctx, mappingKey + masterKey, hashKey).Result()
	return err
}

// return the formatted key
func (h *redisHandler) SetNew(ctx context.Context, uniqueToken string, value interface{}) (string, error) {
	resStr, err := h.client.Get(ctx, partitionKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			resStr = "0"
		} else {
			return "", err
		}
	}

	_, err = h.client.Incr(ctx, partitionKey).Result()
	if err != nil {
		return "", err
	}

	num, err := strconv.Atoi(resStr)
	if err != nil {
		log.Panicf("cannot convert partition value to an integer: %s", err)
	}
	masterKey := strconv.Itoa(num / hashKeyNumber)
	_, err = h.client.HSet(ctx, mappingKey + masterKey, uniqueToken, value).Result()
	return masterKey + "-" + uniqueToken, err
}

type client struct {
	seaweedUrl   string
	redisHandler *redisHandler
	httpClient   *http.Client
}

func (c *client) sendRequestAndForwardResponse(w http.ResponseWriter, req *http.Request) {
	resp, err := c.httpClient.Do(req)
	var urlErr url.Error
	if errors.As(err, &urlErr) {
		log.Printf("http request error: %s/n", urlErr)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	forwardResponse(w, resp)
}

func forwardResponse(w http.ResponseWriter, src *http.Response) {
	w.WriteHeader(src.StatusCode)
	written, err := io.Copy(w, src.Body)
	if err != nil {
		log.Printf("transferred %v bytes, http response error: %s/n", written, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func isSuccessStatus(code int) bool {
	return code >= 200 && code < 300
}

func (c *client) getHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch path := chi.URLParam(r, "path"); path {
		case "status":
			newURL, err := url.Parse(c.seaweedUrl)
			if err != nil {
				log.Panicf("invalid seaweedfs url: %s", err)
			}

			newURL.Path = path
			newURL.RawQuery = r.URL.RawQuery
			req, err := http.NewRequestWithContext(r.Context(), "GET", newURL.String(), nil)
			if err != nil {
				log.Printf("invalid url request: %s/n", err)
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}

			c.sendRequestAndForwardResponse(w, req)
		default:
			key, err := c.redisHandler.Get(r.Context(), path)
			if err != nil {
				switch err {
				case redis.Nil:
					http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
				case ErrInvalidTokenFormat:
					http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				default:
					log.Printf("redis client error: %s/n", err)
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
				return
			}
			newURL, err := url.Parse(c.seaweedUrl)
			if err != nil {
				log.Panicf("invalid seaweedfs url: %s", err)
			}

			newURL.Path = key
			newURL.RawQuery = r.URL.RawQuery
			req, err := http.NewRequestWithContext(r.Context(), "GET", newURL.String(), nil)
			if err != nil {
				log.Printf("invalid url request: %s/n", err)
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}

			for k, v := range r.Header {
				if k == "Authorization" ||
				   k == "Range" ||
				   k == "If-Modified-Since"||
				   k == "If-None-Match" ||
				   k == "Accept-Encoding: gzip" {
					req.Header[k] = v
				}
			}

			c.sendRequestAndForwardResponse(w, req)
		}
	}
}

func (c *client) postHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch path := chi.URLParam(r, "path"); path {
		case "submit":
			newURL, err := url.Parse(c.seaweedUrl)
			if err != nil {
				log.Panicf("invalid seaweedfs url: %s", err)
			}
			newURL.Path = path
			newURL.RawQuery = r.URL.RawQuery

			req, err := http.NewRequestWithContext(r.Context(), "POST", newURL.String(), r.Body)
			if err != nil {
				log.Printf("invalid url request: %s/n", err)
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}

			for k, v := range r.Header {
				if k == "Authorization" ||
				   k == "Content-MD5"||
				   k == "Content-Type" ||
				   k == "Content-Encoding: gzip" ||
				   strings.HasPrefix(k, "Seaweed-") {
					req.Header[k] = v
				}
			}

			resp, err := c.httpClient.Do(req)
			var urlErr url.Error
			if errors.As(err, &urlErr) {
				log.Printf("http request error: %s/n", urlErr)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()

			if !isSuccessStatus(resp.StatusCode) {
				http.Error(w, resp.Status, resp.StatusCode)
				return
			}

			content, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("unable to read response body: %s/n", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			var result FileDetail
			err = json.Unmarshal(content, &result)
			if err != nil {
				log.Printf("unable to decode json body: %s/n", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			fid := result.Fid

			token, err := UniqueToken()
			if err != nil {
				log.Printf("unable to generate unique token: %s/n", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			formattedToken, err := c.redisHandler.SetNew(r.Context(), token, fid)
			if err != nil {
				log.Println(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			fileUrl, err := url.Parse(result.FileUrl)
			if err != nil {
				log.Println(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			fileUrl.Path = formattedToken
			result.Fid = formattedToken
			result.FileUrl = fileUrl.String()
			
			resultJson, err := json.Marshal(result)
			if err != nil {
				log.Println(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Write(resultJson)

		default:
			key, err := c.redisHandler.Get(r.Context(), path)
			if err != nil {
				switch err {
				case redis.Nil:
					http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
				case ErrInvalidTokenFormat:
					http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				default:
					log.Printf("redis client error: %s/n", err)
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
				return
			}

			newURL, err := url.Parse(c.seaweedUrl)
			if err != nil {
				log.Panicf("invalid seaweedfs url: %s", err)
			}
			newURL.Path = key
			newURL.RawQuery = r.URL.RawQuery

			req, err := http.NewRequestWithContext(r.Context(), "POST", newURL.String(), r.Body)
			if err != nil {
				log.Printf("invalid url request: %s/n", err)
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}

			for k, v := range r.Header {
				if k == "Authorization" ||
				   k == "Content-MD5"||
				   k == "Content-Type" ||
				   k == "Content-Encoding: gzip" ||
				   strings.HasPrefix(k, "Seaweed-") {
					req.Header[k] = v
				}
			}

			c.sendRequestAndForwardResponse(w, req)
		}
	}
}

func (c *client) deleteHandler(timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := chi.URLParam(r, "path")

		key, err := c.redisHandler.Get(r.Context(), path)
		if err != nil {
			switch err {
			case redis.Nil:
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			case ErrInvalidTokenFormat:
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			default:
				log.Printf("redis client error: %s/n", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
			return
		}

		newURL, err := url.Parse(c.seaweedUrl)
		if err != nil {
			log.Panicf("invalid seaweedfs url: %s", err)
		}
		newURL.Path = key

		req, err := http.NewRequestWithContext(r.Context(), "DELETE", newURL.String(), nil)
		if err != nil {
			log.Printf("invalid url request: %s/n", err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		for k, v := range r.Header {
			if k == "Authorization" ||
			// Not sure if delete operation accept seaweed header
				strings.HasPrefix(k, "Seaweed-") {
				req.Header[k] = v
			}
		}

		resp, err := c.httpClient.Do(req)
		var urlErr url.Error
		if errors.As(err, &urlErr) {
			log.Printf("http request error: %s/n", urlErr)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if isSuccessStatus(resp.StatusCode) {
			ctx, _ := context.WithTimeout(context.Background(), timeout)
			err = c.redisHandler.Del(ctx, path)
			if err != nil {
				log.Printf("cannot delete redis key: %s\n", err)
			}
		}

		forwardResponse(w, resp)
	}
}

func main() {
	seaweedURL := flag.String("seaweed", "localhost", "url of seaweedfs server")
	redisMaster := flag.String("master", "", "redis master name")
	sentinelNodes := flag.String("nodes", "", "redis sentinel nodes host:port addresses")
	timeoutValue := flag.String("timeout", "3s", "timeout for http request client and redis background deletion")
	address := flag.String("address", ":1337", "address of this server")
	flag.Parse()

	timeout, err := time.ParseDuration(*timeoutValue)
	if err != nil {
		log.Fatalf("invalid timeout value: %s", err)
	}

	redisClient := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: *redisMaster,
		SentinelAddrs: strings.Split(*sentinelNodes, " "),
	})
	httpClient := &http.Client{
		Timeout: timeout,
	}

	handler := client{
		seaweedUrl: *seaweedURL,
		redisHandler: &redisHandler{
			client: redisClient,
		},
		httpClient: httpClient,
	}

	r := chi.NewRouter()

	r.Use(middleware.Heartbeat("/ping"))

	r.Route("/{path}", func(r chi.Router) {
		r.Get("/", handler.getHandler())
		r.Post("/", handler.postHandler())
		r.Delete("/", handler.deleteHandler(timeout))
	})

	http.ListenAndServe(fmt.Sprintf("%s", *address), r)
}