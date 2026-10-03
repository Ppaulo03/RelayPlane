# Evolution Cluster Gateway

> **Architecture Revision:** Control Plane desacoplado com Provider Adapters, ordered command processing, logical/physical fencing, Reconciler e Claim-Check para mídia.

Gateway de controle, roteamento de instâncias e orquestração assíncrona para plataformas de mensageria baseadas em WhatsApp, projetado para operar com **Evolution API** sem acoplamento direto ao provider, ao broker ou à camada de persistência.

A arquitetura adota **Ports & Adapters (Hexagonal Architecture)** para que Evolution API, Redis Streams, RabbitMQ, Kafka, PostgreSQL ou outras tecnologias sejam implementações substituíveis, e não dependências estruturais do domínio.

---

# 1. Objetivo

O projeto fornece um **Control Plane** capaz de:

- distribuir instâncias entre múltiplos nós de execução;
- garantir ownership exclusivo por instância;
- impedir múltiplos sockets concorrentes para a mesma sessão;
- abstrair diferentes versões ou providers de WhatsApp;
- processar envio de mensagens de forma assíncrona;
- aplicar rate limiting por instância;
- receber e normalizar eventos inbound;
- manter idempotência e deduplicação;
- suportar múltiplos brokers;
- permitir atualização gradual de providers;
- centralizar observabilidade e operação do cluster.

A principal regra arquitetural é:

> **Evolution API, Redis, PostgreSQL e qualquer outra infraestrutura são adapters.  
> O domínio do Gateway não deve depender diretamente deles.**

---

# 2. Problema Resolvido

A Evolution API utiliza Baileys para manter sessões do WhatsApp Web.

Uma sessão do WhatsApp deve possuir apenas um socket ativo por credencial. Quando múltiplos containers carregam a mesma sessão simultaneamente, podem ocorrer:

- `440 Connection Replaced`;
- `401 Unauthorized`;
- loops de conexão/desconexão;
- inconsistência de estado;
- disputa de sessão;
- consumo desnecessário de CPU e memória;
- mensagens duplicadas;
- comportamento imprevisível durante failover.

A solução adotada é **sharding lógico com ownership exclusivo**.

Cada instância pertence a exatamente um nó ativo por vez.

---

# 3. Princípios Arquiteturais

O projeto segue os seguintes princípios.

## 3.1 Ownership exclusivo

Para qualquer instância:

```text
instance -> exactly one active owner
```

Exemplo:

```text
inst-A -> node-01
inst-B -> node-01
inst-C -> node-02
inst-D -> node-03
```

Nunca:

```text
inst-A -> node-01
inst-A -> node-02
```

---

## 3.2 Sticky Placement

O balanceador escolhe o nó apenas na criação da instância.

Instâncias conectadas não são automaticamente rebalanceadas.

```text
CREATE INSTANCE
      |
      v
Placement Engine
      |
      v
node-02
```

Depois disso:

```text
inst-A -> node-02
```

permanece estável até uma operação explícita de migração.

---

## 3.3 Providers são substituíveis

O core não sabe que utiliza Evolution API.

Ele conhece apenas contratos canônicos. Operações dependentes do owner recebem um snapshot explícito do assignment:

```go
type Assignment struct {
    InstanceID string
    NodeID     string
    Epoch      int64
}

type MessagingProvider interface {
    CreateInstance(ctx context.Context, req CreateInstanceRequest) (*Instance, error)
    DeleteInstance(ctx context.Context, assignment Assignment) error
    GetInstanceState(ctx context.Context, assignment Assignment) (*InstanceState, error)
    GetPairingCode(ctx context.Context, assignment Assignment) (*PairingCode, error)
    SendMessage(ctx context.Context, assignment Assignment, msg OutboundMessage) (*SendResult, error)
}
```

O `Assignment` evita que `node_id`, `instance_id` e `assignment_epoch` sejam resolvidos de forma independente durante operações críticas.

Implementações possíveis:

```text
Evolution API v2
Evolution API v3
Meta Cloud API
WAHA
Outro provider futuro
```

---

## 3.4 Brokers são substituíveis

O core também não depende diretamente de:

```text
Redis Streams
RabbitMQ
Kafka
SQS
```

O domínio depende de contratos.

Por exemplo:

```go
type Command struct {
    ID           string
    PartitionKey string
    Payload      any
}

type CommandQueue interface {
    Publish(ctx context.Context, command Command) error
    Consume(ctx context.Context, handler CommandHandler) error
}
```

Para comandos outbound de WhatsApp:

```text
PartitionKey = instance_id
```

O contrato exige preservação de ordem por `PartitionKey`. A implementação física dessa propriedade pertence ao adapter do broker.

e:

```go
type EventBus interface {
    Publish(ctx context.Context, event DomainEvent) error
    Subscribe(ctx context.Context, handler EventHandler) error
}
```

---

## 3.5 Persistência também é um adapter

O domínio não executa diretamente:

```text
SELECT
INSERT
UPDATE
```

Ele depende de repositories.

```go
type InstanceRepository interface {
    Create(ctx context.Context, instance Instance) error
    GetByID(ctx context.Context, id string) (*Instance, error)
    AssignNode(ctx context.Context, id string, nodeID string, epoch int64) error
    UpdateDesiredState(ctx context.Context, id string, state DesiredState) error
    UpdateObservedState(ctx context.Context, id string, state ObservedState) error
}
```

Implementação inicial:

```text
PostgreSQL
```

---

## 3.6 Estado desejado e reconciliação

O Control Plane não depende exclusivamente de webhooks para conhecer a condição das sessões.

Cada instância possui:

```text
desired_state
observed_state
```

Um `Reconciler` compara periodicamente o estado desejado persistido no catálogo com o estado observado no provider e executa ações convergentes quando necessário.

Exemplo:

```text
desired_state  = CONNECTED
observed_state = DISCONNECTED

=> reconciliation required
```

O objetivo é obter convergência eventual sem transformar eventos transitórios em fonte única de verdade.

---

## 3.7 Mídia usa Claim-Check Pattern

Arquivos grandes não devem atravessar o broker de comandos em Base64.

O core depende de uma porta de armazenamento de objetos:

```go
type BlobStore interface {
    Put(ctx context.Context, object Object) (*ObjectRef, error)
    Get(ctx context.Context, ref ObjectRef) (ObjectReader, error)
    Delete(ctx context.Context, ref ObjectRef) error
    SignedURL(ctx context.Context, ref ObjectRef, ttl time.Duration) (string, error)
}
```

Adapters possíveis:

```text
S3
MinIO
Google Cloud Storage
Azure Blob Storage
Filesystem para desenvolvimento
```

A fila transporta somente metadados e uma referência ao objeto.

---

# 4. Visão Geral da Arquitetura

```text
                              APPLICATIONS
                                   |
                                   v
                          +------------------+
                          |   LOAD BALANCER  |
                          +--------+---------+
                                   |
                                   v
                    +-----------------------------+
                    |   GATEWAY / CONTROL PLANE   |
                    |                             |
                    | - REST API                  |
                    | - Instance Management       |
                    | - Placement / Ownership     |
                    | - Idempotency               |
                    | - Routing / Rate Policy     |
                    +------+------------+---------+
                           |            |
                +----------+            +------------------+
                |                                      |
                v                                      v
        +------------------+                 +--------------------+
        |    PostgreSQL    |                 |   Command Queue    |
        |                  |                 |                    |
        | desired_state    |                 | partition key =    |
        | observed_state   |                 | instance_id        |
        | ownership/epoch  |                 +---------+----------+
        +--------+---------+                           |
                 ^                                     v
                 |                              +--------------+
                 |                              |   WORKERS    |
                 |                              +------+-------+
                 |                                     |
                 |                           +---------+---------+
                 |                           |                   |
                 |                           v                   v
         +-------+--------+          MessagingProvider       BlobStore
         |   RECONCILER   |                  |                   |
         +-------+--------+                  v                   v
                 |                    +-------------+      +-----------+
                 |                    | Provider    |      | S3/MinIO  |
                 +------------------->| Nodes       |      | /etc.     |
                                      +------+------+      +-----------+
                                             |
                                             v
                                      WhatsApp / Provider
                                             |
                                             v
                                      Provider Webhooks
                                             |
                                             v
                                      Normalize + Dedupe
                                             |
                                             v
                                         Event Bus
                                             |
                                             v
                                       APPLICATIONS
```

A plataforma opera por três loops independentes:

```text
CONTROL LOOP
Desired State -> Reconciler -> Provider -> Observed State

COMMAND LOOP
Request -> Command Queue -> Worker -> Provider

EVENT LOOP
Provider -> Normalize -> Deduplicate -> Event Bus
```

A Evolution API é apenas um adapter de `MessagingProvider`. Redis Streams, RabbitMQ ou Kafka são adapters de mensageria. S3/MinIO são adapters de `BlobStore`. PostgreSQL é a implementação inicial do catálogo persistente.

---

# 5. Arquitetura Hexagonal

A organização principal é:

```text
Core
 |
 v
Ports
 ^
 |
Adapters
```

O core nunca importa implementações externas.

Regra:

```text
core X evolution
core X redis
core X postgres
core X kafka
```

O core pode depender apenas de contratos definidos em `ports`.

---

# 6. Estrutura do Repositório

```text
.
├── cmd/
│   ├── gateway/
│   │   └── main.go
│   ├── worker/
│   │   └── main.go
│   └── reconciler/
│       └── main.go
│
├── internal/
│   ├── core/
│   │   ├── instance/
│   │   ├── messaging/
│   │   ├── routing/
│   │   ├── ownership/
│   │   ├── reconciliation/
│   │   ├── media/
│   │   └── events/
│   │
│   ├── ports/
│   │   ├── provider.go
│   │   ├── command_queue.go
│   │   ├── event_bus.go
│   │   ├── repository.go
│   │   ├── blob_store.go
│   │   ├── idempotency.go
│   │   └── lock.go
│   │
│   ├── adapters/
│   │   ├── providers/
│   │   │   └── evolution/
│   │   │       ├── v2/
│   │   │       │   ├── client.go
│   │   │       │   ├── mapper.go
│   │   │       │   ├── webhook.go
│   │   │       │   └── provider.go
│   │   │       └── v3/
│   │   │           ├── client.go
│   │   │           ├── mapper.go
│   │   │           └── provider.go
│   │   │
│   │   ├── messaging/
│   │   │   ├── redisstreams/
│   │   │   ├── rabbitmq/
│   │   │   └── kafka/
│   │   │
│   │   ├── storage/
│   │   │   ├── s3/
│   │   │   ├── minio/
│   │   │   └── filesystem/
│   │   │
│   │   └── persistence/
│   │       └── postgres/
│   │
│   ├── api/
│   │   └── http/
│   │       ├── handlers/
│   │       ├── middleware/
│   │       ├── routes.go
│   │       └── server.go
│   │
│   ├── worker/
│   │   ├── outbound.go
│   │   └── retry.go
│   │
│   ├── reconciler/
│   │   ├── runner.go
│   │   └── policies.go
│   │
│   ├── ratelimit/
│   ├── idempotency/
│   ├── observability/
│   ├── config/
│   └── models/
│
├── sdk/
│   └── python/
│       ├── pyproject.toml
│       └── whatsapp_client/
│           ├── client.py
│           ├── instances.py
│           ├── messages.py
│           ├── consumer.py
│           └── models.py
│
├── migrations/
├── deploy/
│   ├── docker/
│   └── kubernetes/
│
├── docker-compose.yml
├── Dockerfile
├── go.mod
├── go.sum
└── README.md
```

---

# 7. Modelo de Domínio

## 7.1 Tenant

Representa o cliente da plataforma.

```text
tenant
 |
 +-- instance A
 +-- instance B
 +-- instance C
```

Um tenant pode possuir múltiplas instâncias.

---

## 7.2 Instance

Representa uma sessão lógica de mensageria.

```json
{
  "id": "inst_01JABC...",
  "tenant_id": "tenant_123",
  "provider": "evolution-v2",
  "provider_instance_id": "evo_01JABC...",
  "node_id": "node-v2-01",
  "assignment_epoch": 17,
  "desired_state": "CONNECTED",
  "observed_state": "CONNECTED",
  "last_provider_heartbeat": "2026-10-02T22:00:00Z"
}
```

`desired_state` representa a intenção do Control Plane. `observed_state` representa a condição realmente observada no provider.

---

## 7.3 Provider Node

Representa uma unidade de execução de um provider.

```json
{
  "id": "node-v2-01",
  "provider": "evolution-v2",
  "endpoint": "http://evolution-v2-01:8080",
  "capacity": 100,
  "active_instances": 47,
  "status": "READY"
}
```

O conceito substitui o termo rígido `Evolution Shard` por `Provider Node`.

---

## 7.4 Node Health vs Instance Health

Saúde do nó e saúde da sessão são sinais independentes.

```text
Provider Node
-------------
STARTING
READY
DEGRADED
DRAINING
OFFLINE
```

Uma sessão pode estar desconectada mesmo com o container HTTP saudável:

```text
Instance Connection
-------------------
CREATING
AWAITING_PAIRING
CONNECTING
CONNECTED
DISCONNECTED
LOGGED_OUT
FAILED
```

Portanto:

```text
node = READY
instance = DISCONNECTED
```

é um estado válido e precisa ser detectado pelo Reconciler.

---

# 8. Provider Capabilities

Nem todos os providers precisam implementar as mesmas funcionalidades.

Cada adapter pode declarar suas capacidades.

```go
type ProviderCapabilities struct {
    QRCode      bool
    PairingCode bool
    Groups      bool
    Media       bool
    Templates   bool
    Presence    bool
}
```

Exemplo:

```json
{
  "provider": "evolution-v2",
  "capabilities": {
    "qr_code": true,
    "pairing_code": true,
    "groups": true,
    "media": true,
    "templates": false,
    "presence": true
  }
}
```

Se uma operação não for suportada:

```http
GET /api/v1/instances/{id}/qrcode
```

o Gateway pode responder:

```json
{
  "error": "capability_not_supported"
}
```

---

# 9. Estados de uma Instância

O lifecycle deve ser explícito e separar intenção de observação.

Estado desejado típico:

```text
CONNECTED
DISCONNECTED
DELETED
```

Estado observado típico:

```text
ALLOCATING
CREATING
AWAITING_PAIRING
CONNECTING
CONNECTED
DISCONNECTED
LOGGED_OUT
RECONNECTING
DRAINING
MIGRATING
DELETING
FAILED
```

Fluxo comum de provisioning:

```text
ALLOCATING
    |
    v
CREATING
    |
    +--------> FAILED
    |
    v
AWAITING_PAIRING
    |
    v
CONNECTING
    |
    v
CONNECTED
```

Exemplo de divergência a reconciliar:

```text
desired_state  = CONNECTED
observed_state = DISCONNECTED
```

O estado persistido pelo Control Plane nunca deve ser atualizado apenas por suposição após uma chamada HTTP; ele deve refletir confirmação do provider ou observação reconciliada.

---

# 10. Ownership, Assignment Epoch e Fencing

Cada atribuição de uma instância possui um `assignment_epoch` monotonicamente crescente.

Exemplo inicial:

```text
inst-A
node-01
epoch=1
```

Após migração:

```text
inst-A
node-02
epoch=2
```

Todo comando que depende do owner deve transportar o snapshot do assignment:

```json
{
  "instance_id": "inst-A",
  "assignment": {
    "node_id": "node-02",
    "epoch": 2
  }
}
```

Antes do dispatch, o worker compara o epoch do comando com o assignment atual.

```text
command.epoch != catalog.current_epoch
        |
        v
STALE_COMMAND
```

O comando obsoleto nunca deve chegar ao provider.

## 10.1 Logical Fencing

O `assignment_epoch` fornece fencing lógico dentro da plataforma:

```text
stale command -> rejected
stale worker  -> rejected
stale retry   -> rejected
```

## 10.2 Physical Fencing

O epoch sozinho não encerra um WebSocket já aberto em um nó antigo.

Portanto, antes de ativar um novo owner, o sistema precisa confirmar o encerramento do owner anterior.

```text
old owner active
      |
      v
request shutdown/logout/delete
      |
      v
confirm socket/session fenced
      |
      v
assign epoch+1
      |
      v
activate new owner
```

Caso o provider não suporte fencing token nativo, a política é conservadora: sem confirmação do encerramento do owner anterior, a migração permanece bloqueada.

---

# 11. Placement Engine

O Placement Engine escolhe o nó apenas para novas instâncias.

Critérios possíveis:

```text
capacity
active_instances
memory_pressure
cpu_pressure
health
provider_version
availability_zone
maintenance_state
```

Exemplo simplificado:

```text
node-01: 75 / 100
node-02: 21 / 100
node-03: DRAINING
```

Nova instância:

```text
-> node-02
```

O Placement Engine deve ignorar nós:

```text
OFFLINE
DRAINING
UNHEALTHY
FULL
```

---

# 12. Alocação Atômica

Selecionar e reservar um nó precisa ser uma operação atômica.

Fluxo lógico:

```text
BEGIN TRANSACTION

select candidate node
lock candidate
validate capacity
reserve capacity
create assignment
create instance

COMMIT
```

Isso impede:

```text
Request A -> último slot do node-01
Request B -> mesmo último slot
```

---

# 13. Failover

Na primeira versão, o sistema não deve realizar failover automático agressivo de sessões.

Se um nó ficar indisponível:

```text
node offline
    |
    v
instances unavailable
```

A prioridade inicial deve ser restaurar o mesmo nó.

Isso evita split-brain.

---

## 13.1 Split-Brain

Cenário perigoso:

```text
              WhatsApp
               ^    ^
               |    |
           node-01 node-02
               \    /
                inst-A
```

Isso pode acontecer se o Control Plane considerar um nó morto enquanto seu socket ainda estiver ativo.

Failover automático futuro deve incluir estratégia de fencing.

---

# 14. Migração de Instâncias

Migração é uma operação explícita e stateful.

Máquina de estados recomendada:

```text
CONNECTED
   |
   v
MIGRATION_REQUESTED
   |
   v
FENCING_OLD_OWNER
   |
   +---- failure ----> MIGRATION_BLOCKED
   |
   v
OLD_OWNER_FENCED
   |
   v
ASSIGN_NEW_EPOCH
   |
   v
START_NEW_OWNER
   |
   v
VERIFY_CONNECTION
   |
   v
CONNECTED
```

Regra fundamental:

> Um novo owner não pode tornar-se ativo enquanto o owner anterior não tiver sido fisicamente fenced.

A migração sacrifica disponibilidade temporária em favor de consistência quando não for possível provar que a sessão anterior foi encerrada.

O histórico de assignments deve ser preservado para auditoria:

```text
instance_id
node_id
epoch
assigned_at
released_at
release_reason
```

---

# 15. Estratégia Blue/Green para Providers

A arquitetura permite manter várias versões simultaneamente.

```text
Evolution v2
------------
node-v2-01
node-v2-02

Evolution v3
------------
node-v3-01
node-v3-02
```

Novas instâncias podem ser direcionadas para v3:

```text
new instances
     |
     v
Evolution v3
```

Enquanto instâncias existentes permanecem em v2:

```text
existing instances
     |
     v
Evolution v2
```

Depois podem ser migradas gradualmente.

---

# 16. Outbound Messaging

O envio deve ser assíncrono.

```text
Application
    |
    v
Gateway API
    |
    v
Command Queue
    |
    v
Outbound Worker
    |
    v
Rate Limiter
    |
    v
Provider Adapter
    |
    v
WhatsApp
```

---

# 17. Modelo de Mensagem Outbound

Envelope canônico:

```json
{
  "message_id": "msg_01J...",
  "idempotency_key": "order-92818-confirmation",
  "tenant_id": "tenant_123",
  "instance_id": "inst_456",
  "assignment": {
    "node_id": "node-v2-01",
    "epoch": 12
  },
  "partition_key": "inst_456",
  "type": "text",
  "to": "5562999999999",
  "payload": {
    "text": "Olá! Sua solicitação foi confirmada."
  },
  "trace_id": "trace_01J...",
  "created_at": "2026-10-02T22:00:00Z"
}
```

`partition_key` deve ser igual a `instance_id` para comandos outbound que exigem ordenação por sessão.

O assignment transportado é um snapshot. Antes do envio efetivo, o worker deve validar que ele ainda corresponde ao assignment corrente no catálogo.

---

# 18. Estados de Mensagem Outbound

O sistema não deve assumir que timeout significa falha.

Estados recomendados:

```text
QUEUED
DISPATCHING
ACCEPTED
DELIVERED
READ
FAILED
UNKNOWN
```

`UNKNOWN` representa um resultado ambíguo.

Exemplo:

```text
Worker -> Provider
          |
          +--> WhatsApp envia mensagem
          |
          X resposta HTTP perdida
```

O worker não sabe se deve reenviar.

O estado correto pode ser:

```text
UNKNOWN
```

em vez de retry cego.

---

# 19. Idempotência

Operações críticas devem suportar:

```http
Idempotency-Key: <value>
```

Aplicável a:

```text
create instance
send message
delete instance
migration
```

Tabela sugerida:

```text
idempotency_keys
----------------
key
request_hash
operation
resource_id
result
status
created_at
expires_at
```

---

# 20. Ordering por Instância

Mensagens pertencentes à mesma `instance_id` devem ser despachadas em ordem lógica.

Cenário indesejado:

```text
worker-1 -> mensagem A -> jitter
worker-2 -> mensagem B -> provider

resultado: B chega antes de A
```

A propriedade deve fazer parte do contrato de `CommandQueue`:

```text
PartitionKey = instance_id
```

Não é recomendado criar um Stream permanente por instância como estratégia padrão. O adapter pode particionar instâncias em um conjunto fixo de partitions.

Exemplo conceitual:

```text
partition = hash(instance_id) % N
```

Assim:

```text
inst-A -> partition-3
inst-B -> partition-7
inst-C -> partition-3
```

mas comandos de `inst-A` continuam ordenados:

```text
A1 -> A2 -> A3
```

Implementações possíveis:

```text
Redis Streams -> streams/consumidores particionados por hash
Kafka         -> key = instance_id
RabbitMQ      -> consistent-hash exchange ou shard queues
```

A fila garante ordenação por chave; o worker também deve impedir execução concorrente da mesma chave quando a implementação exigir.

---

# 21. Rate Limiting

Não utilizar uma regra fixa global como:

```text
1 mensagem / 1.5 segundos
```

O rate limit deve ser configurável.

Modelo:

```go
type RatePolicy struct {
    MinInterval   time.Duration
    Burst         int
    MaxPerMinute  int
    MaxConcurrent int
    Cooldown      time.Duration
}
```

Hierarquia:

```text
global
  |
  v
tenant
  |
  v
instance
```

A configuração mais específica pode sobrescrever a mais geral.

---

## 21.1 Mídia e Claim-Check Pattern

Payloads binários volumosos não devem atravessar Redis Streams, RabbitMQ ou Kafka em Base64.

Fluxo recomendado:

```text
Application
    |
    | upload/stream
    v
Blob Store
    |
    | ObjectRef
    v
Gateway
    |
    v
Command Queue
    |
    v
Worker
    |
    | stream/download
    v
MessagingProvider
```

Mensagem com mídia:

```json
{
  "type": "document",
  "payload": {
    "media": {
      "storage": "s3",
      "object_key": "tenant-123/messages/msg-456/file.pdf",
      "content_type": "application/pdf",
      "size": 1839281,
      "sha256": "...",
      "expires_at": "2026-10-03T22:00:00Z"
    },
    "filename": "documento.pdf"
  }
}
```

O worker deve validar antes do envio:

```text
tenant ownership
content length
MIME type
checksum
expiration
allowed file type
```

Objetos temporários devem possuir lifecycle explícito. Após envio confirmado, podem ser removidos ou retidos conforme política de negócio. Objetos abandonados devem expirar automaticamente.

---

# 22. Command Queue vs Event Bus

Mesmo que ambos utilizem inicialmente Redis Streams, semanticamente são diferentes.

## Command Queue

Representa intenção:

```text
"envie esta mensagem"
```

Exemplo:

```text
outbound.messages
```

## Event Bus

Representa algo que aconteceu:

```text
"uma mensagem foi recebida"
```

Exemplo:

```text
inbound.events
```

Essa distinção permite futuramente:

```text
Outbound Commands -> RabbitMQ
Inbound Events    -> Kafka
```

sem alterar o domínio.

---

# 23. Redis Streams Adapter

Uma implementação inicial pode usar:

```text
Redis Streams
```

Streams/partitions sugeridos:

```text
whatsapp:commands:outbound:<partition>
whatsapp:events:inbound
whatsapp:messages:dlq
```

O número de partitions deve ser fixo/configurável e independente da quantidade de instâncias.

Recursos usados:

```text
XADD
XREADGROUP
XACK
XAUTOCLAIM
```

O core, entretanto, não deve conhecer esses comandos.

---

# 24. Retry e Dead Letter Queue

Política de retry deve ser explícita.

Exemplo:

```text
attempt 1 -> immediate
attempt 2 -> +5s
attempt 3 -> +30s
attempt 4 -> +2m
attempt 5 -> DLQ
```

Erros podem ser classificados como:

```text
RETRYABLE
NON_RETRYABLE
AMBIGUOUS
```

Exemplos:

```text
HTTP 503 -> RETRYABLE
invalid number -> NON_RETRYABLE
network timeout after dispatch -> AMBIGUOUS
```

---

# 25. Inbound Events

Fluxo:

```text
Provider
   |
   v
Webhook Adapter
   |
   v
Validate
   |
   v
Normalize
   |
   v
Deduplicate
   |
   v
Event Bus
   |
   v
200 OK
```

Processamento de negócio não deve acontecer dentro da requisição HTTP do webhook.

---

# 26. Anti-Corruption Layer

Eventos específicos da Evolution não devem escapar do adapter.

Evolution:

```json
{
  "event": "messages.upsert",
  "instance": "cliente_abc",
  "data": {}
}
```

Gateway canonical event:

```json
{
  "event_id": "evt_01J...",
  "event_type": "message.received",
  "provider": "evolution-v2",
  "instance_id": "inst_123",
  "tenant_id": "tenant_123",
  "timestamp": "2026-10-02T22:00:00Z",
  "payload": {
    "from": "5562999999999",
    "text": "Olá"
  }
}
```

Consumidores downstream utilizam apenas o modelo canônico.

---

# 27. Deduplicação de Eventos

Webhooks podem ser reenviados.

O gateway deve deduplicar eventos.

Chave possível:

```text
instance_id
+
event_type
+
provider_message_id
+
event_state
```

Exemplo:

```text
inst_123
message.status
wamid_ABC
delivered
```

não deve colidir com:

```text
inst_123
message.status
wamid_ABC
read
```

---

# 28. Segurança do Webhook

Não confiar em:

```text
?shard=node-01
```

como fonte de verdade.

O gateway deve validar:

```text
claimed node
instance
catalog assignment
```

Exemplo:

```text
payload instance = inst-A
claimed node = node-01

catalog:
inst-A -> node-02
```

Resultado:

```text
ownership violation
```

Isso também pode ser usado como sinal de split-brain.

---

# 29. API Pública

A API pública não deve mencionar Evolution.

## Instances

```http
POST /api/v1/instances
GET /api/v1/instances
GET /api/v1/instances/{instance_id}
DELETE /api/v1/instances/{instance_id}
```

## Pairing

```http
GET /api/v1/instances/{instance_id}/qrcode
GET /api/v1/instances/{instance_id}/pairing-code
```

## Lifecycle

```http
POST /api/v1/instances/{instance_id}/reconnect
POST /api/v1/instances/{instance_id}/logout
POST /api/v1/instances/{instance_id}/migrate
```

## Messaging

```http
POST /api/v1/messages/send
GET /api/v1/messages/{message_id}
```

## Media

```http
POST /api/v1/media/uploads
GET /api/v1/media/{media_id}
DELETE /api/v1/media/{media_id}
```

A API pode retornar URL assinada ou instruções de upload direto para o Blob Store, evitando que arquivos grandes sejam mantidos em memória pelo Gateway.

## Operations

```http
GET /api/v1/operations/{operation_id}
```

## Administration

```http
GET /api/v1/nodes
POST /api/v1/nodes/{node_id}/drain
POST /api/v1/nodes/{node_id}/resume
```

---

# 30. Exemplo de Criação de Instância

Request:

```http
POST /api/v1/instances
Idempotency-Key: tenant-123-main-whatsapp
```

```json
{
  "name": "Comercial",
  "provider": "evolution"
}
```

Response:

```json
{
  "id": "inst_01J...",
  "status": "AWAITING_PAIRING",
  "operation_id": "op_01J..."
}
```

O `tenant_id` deve preferencialmente vir do contexto de autenticação.

---

# 31. Exemplo de Envio de Mensagem

```http
POST /api/v1/messages/send
Idempotency-Key: order-92818-confirmation
```

```json
{
  "instance_id": "inst_01J...",
  "to": "5562999999999",
  "type": "text",
  "payload": {
    "text": "Olá! Sua solicitação foi confirmada."
  }
}
```

Response:

```json
{
  "message_id": "msg_01J...",
  "status": "QUEUED"
}
```

---

# 32. Persistência

PostgreSQL é recomendado como source of truth do Control Plane.

Tabelas principais:

```text
tenants
instances
provider_nodes
instance_assignments
operations
outbound_messages
idempotency_keys
event_deduplication
media_objects
```

---

## 32.1 provider_nodes

```text
id
provider
provider_version
endpoint
capacity
active_instances
status
heartbeat_at
created_at
updated_at
```

---

## 32.2 instances

```text
id
tenant_id
provider
provider_instance_id
node_id
assignment_epoch
desired_state
observed_state
last_provider_heartbeat
last_status_change
created_at
updated_at
```

---

## 32.3 instance_assignments

```text
instance_id
node_id
epoch
assigned_at
released_at
release_reason
```

O histórico é append-oriented e deve permitir reconstruir ownership e migrações.

---

## 32.4 outbound_messages

```text
id
tenant_id
instance_id
idempotency_key
node_id
assignment_epoch
partition_key
recipient
type
payload
status
provider_message_id
attempt_count
created_at
updated_at
```

---

## 32.5 operations

```text
id
tenant_id
instance_id
type
status
error_code
error_message
created_at
updated_at
completed_at
```

---

## 32.6 media_objects

```text
id
tenant_id
message_id
storage_driver
object_key
content_type
size
sha256
status
expires_at
created_at
deleted_at
```

---

# 33. Health Checks

O Gateway deve expor:

```http
GET /health/live
GET /health/ready
```

## Liveness

Indica apenas:

```text
process alive
```

## Readiness

Valida dependências críticas:

```text
PostgreSQL
Command Queue
Event Bus
```

---

# 34. Provider Node Heartbeat e Reconciliation

Cada provider node possui heartbeat de infraestrutura.

Estados sugeridos:

```text
STARTING
READY
DEGRADED
DRAINING
OFFLINE
```

Placement só considera nós `READY`.

Entretanto, heartbeat de nó não representa saúde do socket de cada instância.

## 34.1 Session Health

O adapter deve permitir consulta da sessão:

```go
GetInstanceState(ctx context.Context, assignment Assignment) (*InstanceState, error)
```

Sempre que possível, o provider também deve emitir eventos internos como:

```text
instance.status_changed
```

Esses eventos reduzem o tempo de detecção, mas não substituem reconciliação periódica.

## 34.2 Reconciler

O Reconciler compara:

```text
DESIRED STATE
PostgreSQL

vs

ACTUAL STATE
MessagingProvider
```

Exemplo:

```text
catalog:
inst-A desired=CONNECTED
node=node-01
epoch=12

provider:
inst-A observed=DISCONNECTED
```

Resultado:

```text
observed_state = DISCONNECTED
instance.status_changed
reconciliation policy evaluated
```

O Reconciler deve operar de forma idempotente e tolerar execução concorrente entre réplicas por meio de locks/leases ou claim transacional.

## 34.3 Eventual Convergence

Quando o provider está operacional e a política permite a ação, o sistema deve convergir `observed_state` para `desired_state`.

Exemplo:

```text
desired_state  = DELETED
observed_state = CONNECTED
        |
        v
reconciler -> fence/delete provider session
        |
        v
observed_state = DELETED
```

---

# 35. Observabilidade

Métricas sugeridas:

```text
gateway_instances_total
gateway_instances_by_node

provider_node_health
provider_node_active_instances
provider_request_latency_seconds

instance_observed_state_total
instance_desired_state_total
instance_state_divergence_total
instance_reconciliation_total
instance_reconciliation_failed_total
instance_provider_heartbeat_age_seconds

outbound_queue_depth
outbound_partition_lag
outbound_pending_total
outbound_retry_total
outbound_dlq_total
outbound_stale_command_total

inbound_events_total
inbound_duplicates_total

message_dispatch_latency_seconds
message_status_total

ownership_violation_total
assignment_epoch_mismatch_total
physical_fencing_failed_total
migration_blocked_total

blob_store_operations_total
blob_store_bytes_total
media_expired_total

rate_limit_wait_seconds
```

Alertas prioritários:

```text
ownership violation
assignment epoch mismatch
physical fencing failure
migration blocked
persistent desired/observed divergence
provider heartbeat stale
queue partition lag
DLQ growth
```

---

# 36. Logs Estruturados

Todo log relevante deve incluir quando disponível:

```json
{
  "trace_id": "trace_...",
  "tenant_id": "tenant_...",
  "instance_id": "inst_...",
  "message_id": "msg_...",
  "node_id": "node-...",
  "provider": "evolution-v2",
  "assignment_epoch": 12
}
```

Evitar registrar:

```text
tokens
API keys
session secrets
QR credentials
full message bodies
```

sem necessidade explícita.

---

# 37. Tracing Distribuído

Fluxos importantes devem ser correlacionáveis.

Exemplo:

```text
Application
   |
   v
Gateway
   |
   v
Command Queue
   |
   v
Worker
   |
   v
Provider
```

Todos devem compartilhar:

```text
trace_id
```

quando possível.

---

# 38. Segurança Multi-Tenant

O `tenant_id` deve vir preferencialmente do token/autenticação.

Evitar confiar em:

```json
{
  "tenant_id": "qualquer_valor"
}
```

fornecido diretamente pelo cliente.

Todas as operações precisam validar:

```text
authenticated tenant
        ==
resource tenant
```

---

# 39. Secrets

Segredos devem ser gerenciados fora do código.

Exemplos:

```text
GATEWAY_API_KEY
PROVIDER_API_KEY
DATABASE_PASSWORD
REDIS_PASSWORD
WEBHOOK_SECRET
```

Em produção:

```text
Kubernetes Secrets
Docker Secrets
Vault
Cloud Secret Manager
```

---

# 40. Evolution API Adapter

A implementação Evolution deve estar isolada.

```text
internal/adapters/providers/evolution/v2/
```

Responsabilidades:

```text
HTTP client
Evolution authentication
DTO mapping
webhook parsing
error translation
capability mapping
version compatibility
```

O adapter transforma erros específicos:

```text
Evolution error
```

em erros do domínio:

```text
ProviderUnavailable
InstanceNotFound
PairingUnavailable
AuthenticationFailed
AmbiguousDispatch
```

---

# 41. Compatibilidade de Versões

Versões de provider devem ser pinadas.

Evitar:

```text
latest
```

Preferir:

```text
exact version
```

ou:

```text
image digest
```

O projeto deve manter uma matriz de compatibilidade.

Exemplo:

| Gateway | Adapter | Evolution | Status |
|---|---|---|---|
| 1.x | evolution-v2 | 2.x | supported |
| 1.x | evolution-v3 | 3.x | experimental |
| 2.x | evolution-v3 | 3.x | supported |

---

# 42. Upgrade de Provider

Exemplo:

```text
Evolution 2.x
      |
      v
adapter evolution/v2
```

Nova versão:

```text
Evolution 3.x
      |
      v
adapter evolution/v3
```

O restante permanece:

```text
Gateway API
Routing
Ownership
Queue
Rate limiting
Idempotency
SDK
Observability
```

---

# 43. Troca de Broker

Redis inicial:

```text
CommandQueue = RedisStreams
EventBus     = RedisStreams
```

Migração futura:

```text
CommandQueue = RabbitMQ
EventBus     = Kafka
```

O core não muda.

Apenas bindings/configuração/adapters.

---

# 44. Configuração

Exemplo conceitual:

```env
APP_ENV=production
HTTP_PORT=8080

DATABASE_DRIVER=postgres
DATABASE_URL=postgres://...

COMMAND_QUEUE_DRIVER=redisstreams
EVENT_BUS_DRIVER=redisstreams
COMMAND_PARTITIONS=32

REDIS_URL=redis://redis:6379/0

BLOB_STORE_DRIVER=s3
BLOB_STORE_ENDPOINT=http://minio:9000
BLOB_STORE_BUCKET=whatsapp-media
MEDIA_INLINE_MAX_BYTES=262144
MEDIA_DEFAULT_TTL=24h

RECONCILER_ENABLED=true
RECONCILER_INTERVAL=30s

DEFAULT_PROVIDER=evolution-v2

PROVIDER_EVOLUTION_V2_ENABLED=true
PROVIDER_EVOLUTION_V3_ENABLED=false
```

Nós devem preferencialmente ser registrados no catálogo ou definidos por discovery.

---

# 45. Docker Compose de Desenvolvimento

Exemplo conceitual:

```text
gateway
worker
reconciler
postgres
redis
minio

evolution-v2-01
evolution-v2-02
```

Cada node Evolution:

```text
replicas = 1
```

Escala horizontal significa adicionar nós:

```text
node-01
node-02
node-03
```

e não replicar a mesma identidade de shard.

---

# 46. Kubernetes

Uma abordagem possível para provider nodes stateful é:

```text
StatefulSet
```

Exemplo:

```text
evolution-v2-0
evolution-v2-1
evolution-v2-2
```

O Gateway, workers e Reconciler podem ser `Deployment` independentes:

```text
gateway     replicas=N
worker      replicas=N
reconciler  replicas=N
```

O Reconciler precisa de coordenação distribuída para impedir que duas réplicas executem simultaneamente uma transição não idempotente sobre a mesma instância.

porque são projetados para serem stateless.

---

# 47. Gateway Stateless

O Gateway não deve manter em memória o source of truth de:

```text
instance placement
ownership
operations
```

Tudo que precise sobreviver a restart deve residir em:

```text
PostgreSQL
```

ou outro adapter persistente.

Isso permite:

```text
gateway-1
gateway-2
gateway-3
```

atrás do mesmo load balancer.

---

# 48. SDK Python

A aplicação cliente deve consumir apenas a API canônica.

Exemplo:

```python
import asyncio
from whatsapp_client import WhatsAppClusterClient


async def main():
    client = WhatsAppClusterClient(
        base_url="http://gateway.internal:8080",
        api_key="gateway-secret-token",
    )

    instance = await client.instances.get_or_create(
        name="Comercial",
    )

    qr = await client.instances.get_qrcode(
        instance_id=instance.id,
    )

    await client.messages.send_text(
        instance_id=instance.id,
        to="5562999999999",
        text="Olá! Sua solicitação foi confirmada.",
        idempotency_key="order-92818-confirmation",
    )


if __name__ == "__main__":
    asyncio.run(main())
```

O SDK não precisa conhecer:

```text
Evolution endpoint
node ID
Redis Stream
PostgreSQL
Baileys
```

---

# 49. Invariantes do Sistema

As invariantes principais devem possuir testes automatizados.

## Invariante 1

```text
Uma instância possui no máximo um owner ativo.
```

## Invariante 2

```text
Uma mensagem nunca é enviada para um assignment_epoch antigo.
```

## Invariante 3

```text
Uma operação idempotente retorna o mesmo resultado quando repetida.
```

## Invariante 4

```text
Um nó em DRAINING não recebe novas instâncias.
```

## Invariante 5

```text
Eventos duplicados não geram efeitos duplicados.
```

## Invariante 6

```text
O core não importa adapters.
```

## Invariante 7

```text
Comandos pertencentes à mesma instance_id são despachados em ordem.
```

## Invariante 8

```text
Um comando cujo assignment_epoch difere do assignment atual nunca é despachado.
```

## Invariante 9

```text
Um novo owner não pode tornar-se ativo antes do fencing confirmado do owner anterior.
```

## Invariante 10

```text
Provider node health e instance connection health são estados independentes.
```

## Invariante 11

```text
Payloads binários acima do limite inline configurado nunca transitam pelo command broker.
```

## Invariante 12

```text
O Reconciler converge observed_state em direção a desired_state quando o provider está operacional e a política permite a ação.
```

---

# 50. Estratégia de Testes

## Unitários

Testar:

```text
placement
ownership
fencing policy
assignment epoch
state machines
desired/observed reconciliation
rate policies
idempotency
partition routing
media validation
event normalization
```

## Integração

Testar:

```text
PostgreSQL
Redis Streams
Blob Store
Evolution adapter
Webhook
Worker
Reconciler
```

## Contract Tests

Cada adapter de provider deve passar pelo mesmo conjunto de contratos.

Exemplo:

```text
ProviderContractSuite
```

Testes:

```text
create instance
get instance state
pair
send text
send media
delete/fence instance
normalize inbound event
translate provider errors
reject stale assignment when supported
```

Adapters de `CommandQueue` também devem possuir contract tests para:

```text
publish/consume
ack/retry
redelivery
DLQ
ordering by PartitionKey
parallelism across different PartitionKeys
```

Adapters de `BlobStore` devem validar:

```text
put/get/delete
signed URL
checksum
expiration
tenant isolation
```

---

# 51. Testes de Concorrência

Cenários obrigatórios:

```text
2 creates simultâneos
2 sends simultâneos na mesma instance_id
sends simultâneos em instances diferentes
preservação de ordering sob jitter
stale command durante migration
gateway restart
worker restart
reconciler concorrente
redis restart
postgres reconnect
provider timeout
webhook duplicate
stale epoch
node drain
```

Validações importantes:

```text
A1 sempre antes de A2 para a mesma instance
A e B podem executar em paralelo se pertencem a instances diferentes
stale epoch nunca alcança o provider
uma migration nunca ativa dois owners
```

---

# 52. Testes de Falha

Exemplos:

```text
Gateway cai depois de chamar CreateInstance
Worker cai depois de enviar e antes de ACK
Provider responde timeout após possível dispatch
Node HTTP permanece healthy com socket WhatsApp morto
Node perde comunicação com Control Plane mas mantém internet
Fencing do old owner falha
Migration fica MIGRATION_BLOCKED
Webhook é entregue duas vezes
Redis redelivery
Postgres transaction rollback
Blob expira antes do dispatch
Blob checksum inválido
Reconciler executa duas vezes a mesma ação
```

O teste de split-brain deve ser tratado como cenário de primeira classe.

---

# 53. O que não fazer

Evitar:

```text
Gateway chamando SDK Evolution diretamente no core
```

Evitar:

```text
Worker executando XREADGROUP diretamente na regra de negócio
```

Evitar:

```text
tenant_id == instance_id
```

Evitar:

```text
automatic rebalance de sessões conectadas
```

Evitar:

```text
automatic failover sem fencing
```

Evitar:

```text
provider-specific payloads expostos à aplicação
```

Evitar:

```text
Docker image :latest
```

Evitar:

```text
Base64 de arquivos grandes dentro do command broker
```

Evitar:

```text
considerar node health como equivalente a socket/session health
```

Evitar:

```text
ativar novo owner sem physical fencing confirmado
```

---

# 54. Roadmap

## Fase 1 — Core

- [ ] Criar módulo Go.
- [ ] Definir entidades de domínio.
- [ ] Definir `desired_state` e `observed_state`.
- [ ] Definir state machines de lifecycle e migration.
- [ ] Definir `Assignment` e `assignment_epoch`.
- [ ] Definir `MessagingProvider`.
- [ ] Definir `CommandQueue` com `PartitionKey`.
- [ ] Definir `EventBus`.
- [ ] Definir `BlobStore`.
- [ ] Definir repositories.
- [ ] Implementar testes das invariantes.

---

## Fase 2 — Persistência

- [ ] Adicionar PostgreSQL.
- [ ] Criar migrations.
- [ ] Implementar `InstanceRepository`.
- [ ] Implementar `ProviderNodeRepository`.
- [ ] Implementar histórico `instance_assignments`.
- [ ] Implementar `OperationRepository`.
- [ ] Implementar idempotency store.
- [ ] Implementar metadata de `media_objects`.

---

## Fase 3 — Evolution Adapter

- [ ] Implementar `evolution/v2`.
- [ ] Implementar client HTTP.
- [ ] Implementar DTO mappers.
- [ ] Implementar webhook parser.
- [ ] Implementar `GetInstanceState`.
- [ ] Implementar fencing/delete/logout conforme capacidades reais do provider.
- [ ] Implementar error translation.
- [ ] Implementar contract tests.

---

## Fase 4 — Placement, Ownership e Fencing

- [ ] Implementar node catalog.
- [ ] Implementar atomic allocation.
- [ ] Implementar assignment epoch.
- [ ] Implementar stale command validation.
- [ ] Implementar sticky placement.
- [ ] Implementar drain.
- [ ] Implementar physical fencing workflow.
- [ ] Implementar `MIGRATION_BLOCKED`.

---

## Fase 5 — Messaging

- [ ] Implementar Redis Streams adapter.
- [ ] Definir número de command partitions.
- [ ] Implementar `hash(instance_id) -> partition`.
- [ ] Implementar outbound worker.
- [ ] Implementar ACK/retry/DLQ.
- [ ] Implementar ordering por `PartitionKey`.
- [ ] Implementar parallelism entre partitions.
- [ ] Implementar rate limiting.

---

## Fase 6 — Blob Storage e Mídia

- [ ] Implementar adapter S3/MinIO.
- [ ] Implementar Claim-Check Pattern.
- [ ] Definir inline payload threshold.
- [ ] Validar MIME, size, checksum e tenant ownership.
- [ ] Implementar TTL/lifecycle de objetos temporários.
- [ ] Implementar envio streaming de mídia no worker.

---

## Fase 7 — Inbound

- [ ] Criar webhook endpoint.
- [ ] Validar node ownership.
- [ ] Normalizar eventos.
- [ ] Implementar deduplicação.
- [ ] Publicar eventos no Event Bus.
- [ ] Emitir/normalizar `instance.status_changed`.

---

## Fase 8 — Reconciliation

- [ ] Implementar Reconciler.
- [ ] Implementar consulta periódica de session health.
- [ ] Comparar `desired_state` vs `observed_state`.
- [ ] Implementar execução idempotente das políticas.
- [ ] Implementar concorrência segura entre réplicas.
- [ ] Registrar divergência e convergência.

---

## Fase 9 — API e SDK

- [ ] Criar endpoints REST.
- [ ] Implementar autenticação.
- [ ] Implementar isolamento multi-tenant.
- [ ] Criar SDK Python.
- [ ] Criar documentação OpenAPI.

---

## Fase 10 — Observabilidade

- [ ] Prometheus metrics.
- [ ] Structured logs.
- [ ] OpenTelemetry tracing.
- [ ] Dashboards.
- [ ] Alertas de ownership violation.
- [ ] Alertas de assignment epoch mismatch.
- [ ] Alertas de fencing failure/migration blocked.
- [ ] Alertas de desired/observed divergence.
- [ ] Alertas de queue partition lag.
- [ ] Alertas de DLQ.

---

## Fase 11 — Evolução

- [ ] Adapter Evolution v3.
- [ ] Blue/Green.
- [ ] Migration workflow completo.
- [ ] Broker RabbitMQ opcional.
- [ ] Kafka Event Bus opcional.
- [ ] Estratégia avançada de failover com fencing verificável.

---

# 55. Resultado Esperado

Com essa arquitetura, uma troca de:

```text
Redis Streams -> RabbitMQ
```

fica restrita principalmente a:

```text
internal/adapters/messaging/
```

Uma troca:

```text
Evolution v2 -> Evolution v3
```

fica restrita principalmente a:

```text
internal/adapters/providers/evolution/
```

Uma troca:

```text
MinIO -> S3
```

fica restrita principalmente a:

```text
internal/adapters/storage/
```

Uma troca:

```text
Evolution -> outro provider
```

não exige reescrever:

```text
routing
ownership
fencing policy
reconciliation
API
idempotency
rate limiting
ordered command processing
observability
SDK
canonical business events
```

O Control Plane passa a possuir garantias explícitas para sessões stateful:

```text
exclusive ownership
sticky placement
logical fencing
physical fencing before migration
ordered commands per instance
idempotency
canonical events
desired/observed reconciliation
claim-check media transport
```

Providers e infraestrutura tornam-se detalhes substituíveis da plataforma.

---

# 56. Resumo

O Evolution Cluster Gateway não deve ser tratado como um simples proxy para Evolution API.

Ele funciona como um **Control Plane de sessões de mensageria stateful**, organizado em três loops principais:

```text
CONTROL LOOP
Desired State -> Reconciler -> Provider -> Observed State

COMMAND LOOP
Request -> Ordered Command Queue -> Worker -> Provider

EVENT LOOP
Provider -> Normalize -> Deduplicate -> Event Bus
```

As principais propriedades arquiteturais são:

```text
Provider abstraction
Broker abstraction
Blob storage abstraction
Persistent ownership
Sticky placement
Assignment epochs
Logical fencing
Physical fencing
Ordered processing by instance_id
Idempotency
Canonical events
Desired/observed reconciliation
Async messaging
Claim-check media handling
Rate limiting
Observability
Explicit lifecycle
Controlled migration
```

A Evolution API é apenas uma implementação de `MessagingProvider`.

Redis Streams é apenas uma possível implementação de `CommandQueue` e `EventBus`.

S3/MinIO são possíveis implementações de `BlobStore`.

PostgreSQL é a implementação inicial das portas de persistência.

O domínio permanece independente dessas tecnologias.

O resultado é uma plataforma que pode trocar versão do provider, migrar de broker, alterar storage e evoluir a infraestrutura sem reconstruir o sistema do zero, preservando as invariantes de concorrência e ownership que protegem as sessões do WhatsApp.
