/**
 * SyncRead — Real-time Room Server
 * Express + Socket.io
 *
 * Deployment: Render.com / Railway / Fly.io (any Node.js host with WebSocket support)
 * NOTE: Vercel serverless does NOT support persistent WebSocket connections.
 *       Use Render.com free tier instead — it's free and works perfectly.
 */
const express   = require('express')
const http      = require('http')
const { Server } = require('socket.io')
const cors      = require('cors')
const { v4: uuidv4 } = require('uuid')

const app    = express()
const server = http.createServer(app)

// Strip trailing slash so CORS origin matching is always exact
const rawOrigin    = process.env.FRONTEND_URL || '*'
const FRONTEND_URL = rawOrigin === '*' ? '*' : rawOrigin.replace(/\/+$/, '')

const corsOptions = {
  origin: (origin, callback) => {
    // Allow requests with no origin (curl, mobile apps) or matching origin
    if (!origin || FRONTEND_URL === '*' || origin === FRONTEND_URL) {
      callback(null, true)
    } else {
      callback(new Error(`CORS blocked: ${origin}`))
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
}

app.use(cors(corsOptions))
app.options('*', cors(corsOptions))   // handle preflight for all routes
app.use(express.json())

const io = new Server(server, {
  cors: {
    origin: FRONTEND_URL === '*' ? '*' : [FRONTEND_URL],
    methods: ['GET', 'POST'],
    credentials: true,
  },
  pingTimeout:  60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
})

/* ════════════════════════════════════════════════════════════════════
   IN-MEMORY ROOM STORE
   Rooms are kept alive for 24 h then auto-deleted.
════════════════════════════════════════════════════════════════════ */
const rooms = new Map()   // roomId → Room

/**
 * Room schema:
 * {
 *   roomId, ownerId, activeControllerId,
 *   participants: [{ clientId, username, avatarColor, isOnline, socketId }],
 *   book: null | { bookId, title, filename, size },
 *   currentPage, scrollPosition,
 *   highlights: [],
 *   messages:   [],
 *   status: 'waiting' | 'confirming' | 'reading',
 *   confirmations: { [clientId]: true },
 *   createdAt,
 * }
 */
const makeRoom = (roomId, ownerId, ownerUsername, ownerAvatarColor) => ({
  roomId,
  ownerId,
  activeControllerId: ownerId,
  participants: [{
    clientId:    ownerId,
    username:    ownerUsername,
    avatarColor: ownerAvatarColor,
    isOnline:    false,   // becomes true on socket join
    socketId:    null,
  }],
  book:          null,
  currentPage:   0,
  scrollPosition: 0,
  highlights:    [],
  messages:      [],
  status:        'waiting',
  confirmations: {},
  createdAt:     Date.now(),
})

/** Strip internal socket IDs before sending to clients */
const pub = (room) => ({
  ...room,
  participants: room.participants.map(({ socketId, ...p }) => p),
})

/* ════════════════════════════════════════════════════════════════════
   REST — room creation & lookup (used before socket connect)
════════════════════════════════════════════════════════════════════ */
app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size }))

/** POST /rooms  — create a room */
app.post('/rooms', (req, res) => {
  const { roomId, clientId, username, avatarColor } = req.body
  if (!roomId || !clientId) return res.status(400).json({ error: 'roomId and clientId required' })
  if (rooms.has(roomId))    return res.status(409).json({ error: 'Room already exists' })

  const room = makeRoom(roomId, clientId, username || 'Unknown', avatarColor || '#888')
  rooms.set(roomId, room)

  // Auto-expire after 24 h
  setTimeout(() => { if (rooms.has(roomId)) rooms.delete(roomId) }, 24 * 60 * 60 * 1000)

  res.status(201).json({ room: pub(room) })
})

/** GET /rooms/:roomId  — check room exists + fetch state (reconnect) */
app.get('/rooms/:roomId', (req, res) => {
  const room = rooms.get(req.params.roomId)
  if (!room) return res.status(404).json({ error: 'Room not found' })
  res.json({ room: pub(room) })
})

/* ════════════════════════════════════════════════════════════════════
   SOCKET.IO
════════════════════════════════════════════════════════════════════ */
io.on('connection', (socket) => {
  let _roomId    = null
  let _clientId  = null

  /* ── Join / Rejoin ──────────────────────────────────── */
  socket.on('room:join', ({ roomId, clientId, username, avatarColor }, cb) => {
    const room = rooms.get(roomId)
    if (!room) return cb?.({ error: 'Room not found' })

    _roomId   = roomId
    _clientId = clientId

    let p = room.participants.find(x => x.clientId === clientId)
    if (!p) {
      p = { clientId, username, avatarColor, isOnline: true, socketId: socket.id }
      room.participants.push(p)
    } else {
      Object.assign(p, { isOnline: true, socketId: socket.id, username, avatarColor })
    }

    socket.join(roomId)
    // Notify everyone (including the joiner) of the updated participant list
    io.to(roomId).emit('room:participants', pub(room).participants)

    // If room is already reading, send the book info so the new joiner can confirm
    if (room.status === 'reading' || room.status === 'confirming') {
      socket.emit('book:requested', { book: room.book })
    }

    cb?.({ room: pub(room) })
  })

  /* ── Leave ───────────────────────────────────────────── */
  socket.on('room:leave', ({ roomId, clientId }) => {
    _doLeave(roomId, clientId)
  })

  /* ── Delete (owner only) ─────────────────────────────── */
  socket.on('room:delete', ({ roomId, clientId }) => {
    const room = rooms.get(roomId)
    if (!room || room.ownerId !== clientId) return
    io.to(roomId).emit('room:deleted')
    rooms.delete(roomId)
  })

  /* ── Book: owner sets the book ──────────────────────── */
  socket.on('book:set', ({ roomId, clientId, book }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== clientId) return

    room.book          = book
    room.status        = 'confirming'
    room.confirmations = { [clientId]: true }   // controller auto-confirms

    // Tell everyone what book was chosen
    io.to(roomId).emit('book:requested', { book })
    io.to(roomId).emit('book:confirm_status', _confirmStatus(room))

    // If room has only the controller, start immediately
    _checkAllConfirmed(roomId)
  })

  /* ── Viewer confirms they uploaded the PDF ───────────── */
  socket.on('book:confirm', ({ roomId, clientId }) => {
    const room = rooms.get(roomId)
    if (!room || room.status !== 'confirming') return

    room.confirmations[clientId] = true
    io.to(roomId).emit('book:confirm_status', _confirmStatus(room))
    _checkAllConfirmed(roomId)
  })

  /* ── Controller force-starts without waiting ─────────── */
  socket.on('book:force_start', ({ roomId, clientId }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== clientId) return
    _startReading(roomId)
  })

  /* ── Sync: scroll ────────────────────────────────────── */
  socket.on('sync:scroll', ({ roomId, clientId, scrollPosition }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== clientId) return
    room.scrollPosition = scrollPosition
    socket.to(roomId).emit('sync:scroll', { scrollPosition })
  })

  /* ── Sync: page ──────────────────────────────────────── */
  socket.on('sync:page', ({ roomId, clientId, page }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== clientId) return
    room.currentPage = page
    socket.to(roomId).emit('sync:page', { page })
  })

  /* ── Sync: highlight ─────────────────────────────────── */
  socket.on('sync:highlight', ({ roomId, clientId, bookId, highlight }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== clientId) return
    room.highlights.push(highlight)
    socket.to(roomId).emit('sync:highlight', { bookId, highlight })
  })

  /* ── Sync: cursor ────────────────────────────────────── */
  socket.on('sync:cursor', ({ roomId, x, y, page }) => {
    socket.to(roomId).emit('sync:cursor', { clientId: _clientId, x, y, page })
  })

  /* ── Chat ────────────────────────────────────────────── */
  socket.on('chat:message', ({ roomId, message }) => {
    const room = rooms.get(roomId)
    if (!room) return
    const msg = { ...message, id: uuidv4(), timestamp: Date.now() }
    room.messages.push(msg)
    if (room.messages.length > 200) room.messages = room.messages.slice(-200)
    io.to(roomId).emit('chat:message', { message: msg })
  })

  /* ── Role transfer: give up control ─────────────────── */
  socket.on('role:transfer', ({ roomId, fromClientId, toClientId }) => {
    const room = rooms.get(roomId)
    if (!room || room.activeControllerId !== fromClientId) return

    room.activeControllerId = toClientId

    io.to(roomId).emit('role:update', {
      activeControllerId: toClientId,
      participants:       pub(room).participants,
    })
  })

  /* ── Disconnect ──────────────────────────────────────── */
  socket.on('disconnect', () => {
    if (!_roomId || !_clientId) return
    const room = rooms.get(_roomId)
    if (!room) return
    const p = room.participants.find(x => x.clientId === _clientId)
    if (p) { p.isOnline = false; p.socketId = null }
    socket.to(_roomId).emit('room:participants', pub(room).participants)
  })

  /* ── Internal helpers ────────────────────────────────── */
  const _doLeave = (roomId, clientId) => {
    const room = rooms.get(roomId)
    if (!room) return
    room.participants = room.participants.filter(x => x.clientId !== clientId)
    socket.to(roomId).emit('room:participants', pub(room).participants)
    socket.leave(roomId)
    _roomId   = null
    _clientId = null
  }

  const _checkAllConfirmed = (roomId) => {
    const room = rooms.get(roomId)
    if (!room || room.status !== 'confirming') return
    const allOnline = room.participants.filter(p => p.isOnline)
    if (allOnline.length === 0) return
    const allConfirmed = allOnline.every(p => room.confirmations[p.clientId])
    if (allConfirmed) _startReading(roomId)
  }

  const _startReading = (roomId) => {
    const room = rooms.get(roomId)
    if (!room) return
    room.status = 'reading'
    io.to(roomId).emit('book:start', {
      book:          room.book,
      currentPage:   room.currentPage,
      scrollPosition: room.scrollPosition,
      highlights:    room.highlights,
    })
  }

  const _confirmStatus = (room) => ({
    confirmed: Object.keys(room.confirmations).length,
    total:     room.participants.length,
    details:   room.confirmations,
  })
})

/* ════════════════════════════════════════════════════════════════════
   START
════════════════════════════════════════════════════════════════════ */
const PORT = process.env.PORT || 3001
server.listen(PORT, () => console.log(`SyncRead backend ▶  http://localhost:${PORT}`))
