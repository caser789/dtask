package dtask

import "time"

const EtcdQueryTimeout = 3 * time.Second
const EtcdCampaignTimeout = 30 // time.Second
const EtcdKeyTTL = 10
const EtcdKeepAliveRetryInterval = 5 * time.Second

const NodePath = "app_name/node"
const Separator = "/"
