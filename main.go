package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

type Config struct {
	ListenPort    string   `json:"listen_port"`
	ShutdownPort  string   `json:"shutdown_port"`
	SeedServers   []string `json:"seed_servers"`
	CheckInterval int      `json:"check_interval"`
}

type Server struct {
	Config       Config
	ActivePeers  map[string]time.Time
	LastChecked  map[string]time.Time
	SelfAddress  string
	mu           sync.Mutex
}

type ServerInfo struct {
	Status string            `json:"status"`
	Peers  map[string]string `json:"peers"`
	ID     string            `json:"id"`
}

type PeerList struct {
	Peers map[string]time.Time `json:"peers"`
}

func NewServer(config Config) *Server {
	selfAddress := getSelfAddress(config.ListenPort)
	
	server := &Server{
		Config:      config,
		ActivePeers: make(map[string]time.Time),
		LastChecked: make(map[string]time.Time),
		SelfAddress: selfAddress,
	}

	// Load peer list on startup
	if err := server.loadPeerList(); err != nil {
		log.Printf("Error loading peer list: %v", err)
	}

	return server
}

func getSelfAddress(port string) string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("Error getting interface addresses: %v", err)
		return "localhost:" + port
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":" + port
			}
		}
	}

	return "localhost:" + port
}

func (s *Server) normalizeAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	if host == "localhost" || host == "127.0.0.1" {
		return s.SelfAddress
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return s.SelfAddress
	}
	return net.JoinHostPort(host, port)
}

func (s *Server) checkPeer(peer string) {
	peer = s.normalizeAddress(peer)
	if peer == s.SelfAddress {
		return // Don't check self
	}
	url := fmt.Sprintf("http://%s/status?id=%s", peer, s.SelfAddress)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error checking peer %s: %v", peer, err)
		// Don't delete the peer, just mark it as inactive
		s.ActivePeers[peer] = time.Time{}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var info ServerInfo
		if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
			log.Printf("Error decoding response from %s: %v", peer, err)
			return
		}
		s.ActivePeers[peer] = time.Now()
		log.Printf("Peer %s (ID: %s) is active", peer, info.ID)

		// Update our peer list with the peers reported by this server
		for remotePeer, lastSeen := range info.Peers {
			if remotePeer != s.SelfAddress && remotePeer != peer {
				parsedTime, _ := time.Parse(time.RFC3339, lastSeen)
				s.ActivePeers[remotePeer] = parsedTime
			}
		}

		// Save the updated peer list
		if err := s.savePeerList(); err != nil {
			log.Printf("Error saving peer list: %v", err)
		}
	} else {
		log.Printf("Peer %s is not active (status code: %d)", peer, resp.StatusCode)
		// Don't delete the peer, just mark it as inactive
		s.ActivePeers[peer] = time.Time{}
	}
	s.LastChecked[peer] = time.Now()
}

func (s *Server) startPeerChecker() {
	ticker := time.NewTicker(time.Duration(s.Config.CheckInterval) * time.Second)
	log.Printf("Starting peer checker (interval: %d seconds)", s.Config.CheckInterval)
	log.Printf("Self address: %s", s.SelfAddress)
	go func() {
		for range ticker.C {
			log.Println("Checking peers...")
			
			checkedPeers := make(map[string]bool)
			
			// Check seed servers
			for _, seed := range s.Config.SeedServers {
				if seed != s.SelfAddress && !checkedPeers[seed] {
					s.checkPeer(seed)
					checkedPeers[seed] = true
				}
			}
			
			// Check known active peers
			for peer := range s.ActivePeers {
				if peer != s.SelfAddress && !checkedPeers[peer] {
					s.checkPeer(peer)
					checkedPeers[peer] = true
				}
			}
			
			// Save peer list after each check
			if err := s.savePeerList(); err != nil {
				log.Printf("Error saving peer list: %v", err)
			}
		}
	}()
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	remoteID := s.normalizeAddress(r.URL.Query().Get("id"))
	if remoteID != "" {
		log.Printf("Received ping from server %s", remoteID)
		
		s.mu.Lock()
		s.ActivePeers[remoteID] = time.Now()
		s.mu.Unlock()

		// Save the updated peer list
		if err := s.savePeerList(); err != nil {
			log.Printf("Error saving peer list: %v", err)
		}
	}

	info := ServerInfo{
		Status: "ok",
		Peers:  make(map[string]string),
		ID:     s.SelfAddress,
	}

	for peer, lastSeen := range s.ActivePeers {
		info.Peers[peer] = lastSeen.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(info)
}

func recoveryMiddleware(next http.HandlerFunc, errorLogger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Create a stack trace
				stack := make([]byte, 4096)
				stack = stack[:runtime.Stack(stack, false)]

				// Log the error and stack trace
				errorLogger.Printf("Panic: %v\n%s", err, stack)

				// Return an internal server error to the client
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	}
}

func main() {
	// Check if a config file is specified as a command-line argument
	if len(os.Args) < 2 {
		log.Fatal("Please specify a config file as an argument")
	}
	configFile := os.Args[1]

	// Load configuration
	config, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create logs directory if it doesn't exist
	logsDir := "./logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		log.Fatalf("Failed to create logs directory: %v", err)
	}

	// Set up error logging to stderr
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Replace the default logger to log to stdout
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Printf("Starting server on port %s", config.ListenPort)
	server := NewServer(config)

	mainServer := &http.Server{
		Addr:    ":" + config.ListenPort,
		Handler: http.DefaultServeMux,
	}

	// Set up routes
	http.HandleFunc("/status", recoveryMiddleware(server.handleStatus, errorLogger))

	// Start the peer checker
	server.startPeerChecker()

	// Channel to signal shutdown
	shutdown := make(chan struct{})

	// Start the shutdown listener
	go func() {
		listener, err := net.Listen("tcp", ":" + config.ShutdownPort)
		if err != nil {
			log.Fatalf("Failed to start shutdown listener: %v", err)
		}
		defer listener.Close()

		log.Printf("Shutdown listener started on port %s", config.ShutdownPort)
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting shutdown connection: %v", err)
			return
		}
		conn.Close()
		log.Println("Shutdown signal received")
		close(shutdown)
	}()

	// Start the main server
	go func() {
		log.Printf("Main server listening on port %s", config.ListenPort)
		if err := mainServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-shutdown

	log.Println("Starting shutdown...")

	// Give outstanding requests a deadline for completion
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the server
	if err := mainServer.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}

func loadConfig(filename string) (Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return Config{}, err
	}

	return config, nil
}

func (s *Server) savePeerList() error {
	peerList := PeerList{
		Peers: make(map[string]time.Time),
	}
	for peer, lastSeen := range s.ActivePeers {
		if peer != s.SelfAddress {
			peerList.Peers[peer] = lastSeen
		}
	}
	data, err := json.MarshalIndent(peerList, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("peer_list.json", data, 0644)
}

func (s *Server) loadPeerList() error {
	data, err := os.ReadFile("peer_list.json")
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, not an error
			return nil
		}
		return err
	}

	var peerList PeerList
	if err := json.Unmarshal(data, &peerList); err != nil {
		return err
	}

	// Merge loaded peers with seed servers
	for _, seed := range s.Config.SeedServers {
		if _, exists := peerList.Peers[seed]; !exists {
			peerList.Peers[seed] = time.Time{}
		}
	}

	s.ActivePeers = peerList.Peers
	return nil
}