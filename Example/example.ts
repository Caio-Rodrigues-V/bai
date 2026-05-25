import { Boom } from '@hapi/boom'
import NodeCache from '@cacheable/node-cache'
import readline from 'readline'
import QRCode from 'qrcode-terminal'
import fs from 'fs'
import makeWASocket, {
	CacheStore,
	DEFAULT_CONNECTION_CONFIG,
	DisconnectReason,
	fetchLatestBaileysVersion,
	makeCacheableSignalKeyStore,
	proto,
	useMultiFileAuthState,
	WAMessageContent,
	WAMessageKey
} from '../src'
import P from 'pino'

const logger = P({
	level: 'trace',
	transport: {
		targets: [
			{
				target: 'pino-pretty',
				options: { colorize: true },
				level: 'trace'
			},
			{
				target: 'pino/file',
				options: { destination: './wa-logs.txt' },
				level: 'trace'
			}
		]
	}
})

const usePairingCode = process.argv.includes('--use-pairing-code')
const msgRetryCounterCache = new NodeCache() as CacheStore

const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
const question = (text: string) => new Promise<string>((resolve) => rl.question(text, resolve))

const saveJsonLine = (file: string, data: unknown) => {
	fs.appendFileSync(file, JSON.stringify(data, null, 2) + '\n\n')
}

const decodeTransportEndpoint = (buf: Buffer) => {
	if(buf.length === 6) {
		const ip = `${buf[0]}.${buf[1]}.${buf[2]}.${buf[3]}`
		const port = buf.readUInt16BE(4)
		return { type: 'ipv4', ip, port, hex: buf.toString('hex') }
	}

	if(buf.length === 18) {
		const parts = []
		for(let i = 0; i < 16; i += 2) {
			parts.push(buf.readUInt16BE(i).toString(16))
		}

		const ip = parts.join(':')
		const port = buf.readUInt16BE(16)
		return { type: 'ipv6', ip, port, hex: buf.toString('hex') }
	}

	return { type: 'unknown', length: buf.length, hex: buf.toString('hex') }
}

const printCallNode = (node: any) => {
	console.log('\n=========== CALL NODE =========')
	console.log('TAG:', node.tag)
	console.log('ATTRS:', node.attrs)

	if(node.content) {
		for(const item of node.content) {
			console.log('\nSUBTAG:', item.tag)
			console.log('SUBATTRS:', item.attrs)

			if(item.content) {
				for(const child of item.content) {
					console.log('  CHILD:', child.tag, child.attrs)

					if(Buffer.isBuffer(child.content)) {
						console.log('  BUFFER:', decodeTransportEndpoint(child.content))
					} else if(child.content) {
						console.dir(child.content, { depth: null })
					}
				}
			}
		}
	}

	console.log('===============================\n')
}

const startSock = async() => {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info')

	const { version, isLatest } = await fetchLatestBaileysVersion()
	logger.debug({ version: version.join('.'), isLatest }, 'using latest WA version')

	const sock = makeWASocket({
		version,
		logger,
		waWebSocketUrl: process.env.SOCKET_URL ?? DEFAULT_CONNECTION_CONFIG.waWebSocketUrl,
		auth: {
			creds: state.creds,
			keys: makeCacheableSignalKeyStore(state.keys, logger)
		},
		msgRetryCounterCache,
		generateHighQualityLinkPreview: true,
		getMessage
	})

	sock.ws.on('CB:call', node => {
		printCallNode(node)
		saveJsonLine('calls.json', node)
	})

	sock.ws.on('CB:call,offer', node => {
		console.log('\n=========== OFFER NODE =========')
		console.dir(node, { depth: null })
		console.log('================================\n')
		saveJsonLine('offers.json', node)
	})

	sock.ws.on('CB:call,accept', node => {
		console.log('\n=========== ACCEPT NODE =========')
		console.dir(node, { depth: null })
		console.log('=================================\n')
		saveJsonLine('accepts.json', node)
	})

	sock.ws.on('CB:call,transport', node => {
		console.log('\n=========== TRANSPORT NODE =========')
		console.dir(node, { depth: null })
		console.log('====================================\n')
		saveJsonLine('transports.json', node)
	})

	sock.ws.on('CB:call,relaylatency', node => {
		console.log('\n=========== RELAY LATENCY NODE =========')
		console.dir(node, { depth: null })
		console.log('========================================\n')
		saveJsonLine('relaylatency.json', node)
	})

	sock.ev.process(async(events) => {
		if(events['connection.update']) {
			const update = events['connection.update']
			const { connection, lastDisconnect, qr } = update

			if(qr) {
				console.log('\n======= QR CODE =======\n')
				QRCode.generate(qr, { small: true })
				console.log('\nEscaneie no WhatsApp > Dispositivos conectados\n')
			}

			if(connection === 'close') {
				if((lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut) {
					startSock()
				} else {
					logger.fatal('Connection closed. You are logged out.')
				}
			}

			if(connection === 'open') {
				console.log('\n✅ CONECTADO. Agora faça uma ligação e mantenha ativa por 20 segundos.\n')
			}

			if(qr && usePairingCode && !sock.authState.creds.registered) {
				const phoneNumber = await question('Digite seu número com DDI, ex: 5511999999999:\n')
				const code = await sock.requestPairingCode(phoneNumber)
				console.log(`Pairing code: ${code}`)
			}

			logger.debug(update, 'connection update')
		}

		if(events['creds.update']) {
			await saveCreds()
		}

		if(events['call']) {
			console.log('\n================ CALL EVENT ================')
			console.dir(events['call'], { depth: null })
			console.log('============================================\n')

			saveJsonLine('call-events.json', events['call'])
			logger.debug(events['call'], 'call event fired')
		}

		if(events['messaging-history.set']) {
			const { chats, contacts, messages, isLatest, progress, syncType } = events['messaging-history.set']

			logger.debug({
				contacts: contacts.length,
				chats: chats.length,
				messages: messages.length,
				isLatest,
				progress,
				syncType: syncType?.toString()
			}, 'messaging-history.set event fired')
		}

		if(events['messages.upsert']) {
			logger.debug(events['messages.upsert'], 'messages.upsert fired')
		}
	})

	return sock

	async function getMessage(key: WAMessageKey): Promise<WAMessageContent | undefined> {
		return proto.Message.create({ conversation: 'test' })
	}
}

startSock()