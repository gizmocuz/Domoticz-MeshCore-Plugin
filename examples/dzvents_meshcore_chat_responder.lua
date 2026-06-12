--[[
    MeshCore Chat Responder v2 (dzVents bridge)
    ============================================
    Compatible with MeshCore plugin v1.0.3+ which exposes three bridge
    devices when a Command Bridge Channel (Mode3) is configured:

      - MeshCore Command In  (Text, unit 5)   -- JSON payload per inbound command
      - MeshCore Reply       (Text, unit 6)   -- dzVents writes JSON reply here
      - MeshCore Send        (Switch, unit 7, Push On) -- turn On to dispatch

    Inbound JSON from the plugin (written to MeshCore Command In):
      {"id":<int>, "seq":<int>, "cmd":"!command", "sender":"name",
       "pubkey":"prefix", "channel":"channelname", "snr":<float>, "ts":<int>}

    Reply JSON written to MeshCore Reply:
      First message:     {"id":<same id>, "text":"reply text"}
      Queued follow-ups: {"to":"#channelname", "text":"reply text"}

    The plugin removes the origin record after the first id-based reply, so
    queued messages use the explicit "to" override with the channel name.
    Messages are queued one-per-minute to avoid LoRa TX overlap.

    The plugin only forwards messages on the configured command channel that
    start with the command prefix (default: !), so this script never sees
    its own replies or unrelated traffic.

    REQUIREMENTS
    ------------
    - MeshCore plugin v1.0.3+
    - A channel name configured in Mode3 (Command Bridge Channel)
    - The three bridge devices must exist in Domoticz

    SUPPORTED COMMANDS (case-insensitive)
    --------------------------------------
      !help             -- list available commands
      !status           -- full summary (climate, weather, energy, home)
      !climate          -- indoor climate + heating status
      !weather          -- outdoor weather conditions
      !energy           -- power, solar, battery, gas
      !home             -- water usage, presence
      !device <name>    -- query any Domoticz device by name
      !switches         -- list all switches and their states
      !temp             -- all temperature sensors

    CONFIGURATION
    -------------
    1. Set the MESHCORE_* constants to match your bridge device names.
    2. Fill in the DEVICES table with your actual Domoticz device names.
       Any device set to nil is silently skipped — you do not need them all.
]]--

-- ─────────────────────────────────────────────────────────────────────────────
-- CONFIGURATION – adjust these to match your setup
-- ─────────────────────────────────────────────────────────────────────────────
local MESHCORE_INBOX  = 'MeshCore Command In'    -- JSON command input  (Text, unit 5)
local MESHCORE_REPLY  = 'MeshCore Reply'         -- JSON reply output   (Text, unit 6)
local MESHCORE_SEND   = 'MeshCore Send'          -- Dispatch trigger    (Switch, unit 7)
local CMD_PREFIX      = '!'                      -- command prefix character

-- Device names – set to nil to skip any device you do not have
local DEVICES = {
    tempIndoor      = 'Temperature - Living Room',  -- Temp (+Humidity) sensor
    tempBathroom    = 'Temperature - Bathroom',     -- Temp (+Humidity) sensor (optional)
    tempOutdoor     = 'Temperature - Outside',      -- Temp (+Humidity) sensor
    thermostat      = 'Thermostat',                 -- Setpoint device
    heatpump        = 'Heat Pump Status',           -- Text device (optional)
    ventilation     = 'Ventilation',                -- Selector switch (optional)
    power           = 'Power',                      -- P1 Smart Meter (energy)
    solar           = 'Solar Power',                -- kWh / Solar device (optional)
    homeBattery     = 'Home Battery',               -- Percentage device (optional)
    gas             = 'Gas',                        -- P1 Smart Meter (gas, optional)
    water           = 'Water Meter',                -- Counter device (optional)
    presence        = 'Presence',                   -- Selector switch (optional)
    wind            = 'Wind',                       -- Wind device (optional)
    rain            = 'Rain',                       -- Rain device (optional)
}
-- ─────────────────────────────────────────────────────────────────────────────

return {
    active = true,

    on = {
        timer = {
            'every minute',
        },
        devices = {
            MESHCORE_INBOX,
        },
    },

    logging = {
        level = domoticz.LOG_INFO,
        marker = 'MeshChat',
    },

    data = {
        -- Each entry: { channel = "name", text = "..." }
        -- (id-based routing is only used for the first reply of a request)
        pendingReplies = { initial = {} },
        lastHandledSeq = { initial = 0 },
    },

    execute = function(dz, triggeredItem)

        -- ─────────────────────────────────────────────────────────────────
        -- Helpers
        -- ─────────────────────────────────────────────────────────────────

        local function safeDev(name)
            if (name == nil or name == '') then return nil end
            local ok, d = pcall(dz.devices, name)
            if ok and d then return d end
            return nil
        end

        local replyDev = safeDev(MESHCORE_REPLY)
        local sendDev  = safeDev(MESHCORE_SEND)

        if (replyDev == nil) then
            dz.log('MeshCore Reply device not found: ' .. MESHCORE_REPLY, dz.LOG_ERROR)
            return
        end
        if (sendDev == nil) then
            dz.log('MeshCore Send device not found: ' .. MESHCORE_SEND, dz.LOG_ERROR)
            return
        end

        -- Send via id-based routing (first message of a request).
        -- The plugin resolves the origin channel from the id.
        local function sendReplyById(id, message)
            local payload = dz.utils.toJSON({ id = id, text = message })
            dz.log('Replying (id=' .. tostring(id) .. '): ' .. message, dz.LOG_INFO)
            replyDev.updateText(payload)
            sendDev.switchOn()
        end

        -- Send via explicit channel (queued follow-up messages).
        -- The plugin origin record is consumed after the first id-based send,
        -- so subsequent messages in a multi-part reply use "to" directly.
        local function sendReplyTo(channel, message)
            local payload = dz.utils.toJSON({ to = '#' .. channel, text = message })
            dz.log('Replying (to=#' .. channel .. '): ' .. message, dz.LOG_INFO)
            replyDev.updateText(payload)
            sendDev.switchOn()
        end

        local function round(num, dec)
            if (num == nil) then return 0 end
            local m = 10 ^ (dec or 1)
            return math.floor(num * m + 0.5) / m
        end

        local function safeVal(val, fallback)
            if (val == nil) then return fallback or 0 end
            return val
        end

        local function lower(s)
            if (s == nil) then return '' end
            return string.lower(s)
        end

        local function trim(s)
            if (s == nil) then return '' end
            return s:match('^%s*(.-)%s*$')
        end

        -- ─────────────────────────────────────────────────────────────────
        -- Command handlers
        -- ─────────────────────────────────────────────────────────────────

        local function cmdHelp()
            return 'Commands: '
                .. CMD_PREFIX .. 'status | '
                .. CMD_PREFIX .. 'climate | '
                .. CMD_PREFIX .. 'weather | '
                .. CMD_PREFIX .. 'energy | '
                .. CMD_PREFIX .. 'home | '
                .. CMD_PREFIX .. 'device <name> | '
                .. CMD_PREFIX .. 'switches | '
                .. CMD_PREFIX .. 'temp | '
                .. CMD_PREFIX .. 'help'
        end

        local function cmdClimate()
            local parts = {}
            local indoor = safeDev(DEVICES.tempIndoor)
            if (indoor) then
                table.insert(parts, 'Indoor ' .. round(indoor.temperature) .. 'C, ' .. safeVal(indoor.humidity, 0) .. '%')
            end
            local bath = safeDev(DEVICES.tempBathroom)
            if (bath) then
                table.insert(parts, 'Bathroom ' .. round(bath.temperature) .. 'C, ' .. safeVal(bath.humidity, 0) .. '%')
            end
            local th = safeDev(DEVICES.thermostat)
            if (th) then
                table.insert(parts, 'Thermostat ' .. safeVal(th.setPoint, '?') .. 'C')
            end
            local hp = safeDev(DEVICES.heatpump)
            if (hp) then
                table.insert(parts, 'Heat pump: ' .. hp.text)
            end
            local vent = safeDev(DEVICES.ventilation)
            if (vent) then
                table.insert(parts, 'Ventilation: ' .. (vent.levelName or vent.state))
            end
            if (#parts == 0) then return 'No climate devices found' end
            return 'Climate: ' .. table.concat(parts, ' | ')
        end

        local function cmdWeather()
            local parts = {}
            local outdoor = safeDev(DEVICES.tempOutdoor)
            if (outdoor) then
                table.insert(parts, round(outdoor.temperature) .. 'C, ' .. safeVal(outdoor.humidity, 0) .. '%')
            end
            local wind = safeDev(DEVICES.wind)
            if (wind) then
                table.insert(parts, 'Wind ' .. safeVal(wind.directionString, '?') .. ' ' .. round(wind.speed) .. ' m/s')
            end
            local rain = safeDev(DEVICES.rain)
            if (rain and tonumber(rain.rain or 0) > 0) then
                table.insert(parts, 'Rain ' .. rain.rain .. ' mm')
            end
            if (#parts == 0) then return 'No weather devices found' end
            return 'Weather: ' .. table.concat(parts, ' | ')
        end

        local function cmdEnergy()
            local parts = {}
            local pw = safeDev(DEVICES.power)
            if (pw) then
                local use = pw.usage or 0
                local del = pw.usageDelivered or 0
                if (del > 0) then
                    table.insert(parts, 'Delivering ' .. del .. 'W')
                else
                    table.insert(parts, 'Using ' .. use .. 'W')
                end
            end
            local sol = safeDev(DEVICES.solar)
            if (sol) then
                table.insert(parts, 'Solar ' .. (sol.WhActual or 0) .. 'W')
            end
            local hbat = safeDev(DEVICES.homeBattery)
            if (hbat) then
                table.insert(parts, 'Battery ' .. hbat.percentage .. '%')
            end
            local gas = safeDev(DEVICES.gas)
            if (gas) then
                table.insert(parts, 'Gas today ' .. gas.counterToday)
            end
            if (#parts == 0) then return 'No energy devices found' end
            return 'Energy: ' .. table.concat(parts, ' | ')
        end

        local function cmdHome()
            local parts = {}
            local water = safeDev(DEVICES.water)
            if (water) then
                table.insert(parts, 'Water today ' .. water.counterToday)
            end
            local pres = safeDev(DEVICES.presence)
            if (pres) then
                table.insert(parts, 'Presence: ' .. (pres.levelName or pres.state))
            end
            if (#parts == 0) then return 'No home devices found' end
            return 'Home: ' .. table.concat(parts, ' | ')
        end

        local function cmdDevice(name)
            if (name == nil or name == '') then
                return 'Usage: ' .. CMD_PREFIX .. 'device <name>'
            end
            local d = safeDev(name)
            if (d == nil) then
                return 'Device "' .. name .. '" not found'
            end
            local info = d.name .. ': '
            if (d.temperature ~= nil) then
                info = info .. round(d.temperature) .. 'C'
                if (d.humidity ~= nil) then
                    info = info .. ', ' .. d.humidity .. '%'
                end
            elseif (d.percentage ~= nil) then
                info = info .. d.percentage .. '%'
            elseif (d.setPoint ~= nil) then
                info = info .. d.setPoint .. 'C'
            elseif (d.levelName ~= nil and d.levelName ~= '') then
                info = info .. d.levelName
            elseif (d.state ~= nil) then
                info = info .. d.state
            elseif (d.text ~= nil and d.text ~= '') then
                info = info .. d.text
            else
                info = info .. (d.sValue or 'unknown')
            end
            if (d.lastUpdate ~= nil) then
                info = info .. ' (updated: ' .. d.lastUpdate.raw .. ')'
            end
            return info
        end

        local function cmdSwitches()
            local parts = {}
            local count = 0
            dz.devices().forEach(function(d)
                if (d.switchType ~= nil and d.switchType ~= '') then
                    count = count + 1
                    if (count <= 15) then
                        table.insert(parts, d.name .. '=' .. d.state)
                    end
                end
            end)
            if (count == 0) then return 'No switches found' end
            local msg = 'Switches: ' .. table.concat(parts, ' | ')
            if (count > 15) then
                msg = msg .. ' (+' .. (count - 15) .. ' more)'
            end
            return msg
        end

        local function cmdTemp()
            local parts = {}
            local count = 0
            dz.devices().forEach(function(d)
                if (d.temperature ~= nil) then
                    count = count + 1
                    if (count <= 10) then
                        local entry = d.name .. ' ' .. round(d.temperature) .. 'C'
                        if (d.humidity ~= nil) then
                            entry = entry .. '/' .. d.humidity .. '%'
                        end
                        table.insert(parts, entry)
                    end
                end
            end)
            if (count == 0) then return 'No temperature sensors found' end
            local msg = 'Temp: ' .. table.concat(parts, ' | ')
            if (count > 10) then
                msg = msg .. ' (+' .. (count - 10) .. ' more)'
            end
            return msg
        end

        local function cmdStatus()
            local msgs = {}
            local c = cmdClimate()
            if (c and not c:find('not found')) then table.insert(msgs, c) end
            local w = cmdWeather()
            if (w and not w:find('not found')) then table.insert(msgs, w) end
            local e = cmdEnergy()
            if (e and not e:find('not found')) then table.insert(msgs, e) end
            local h = cmdHome()
            if (h and not h:find('not found')) then table.insert(msgs, h) end
            if (#msgs == 0) then
                table.insert(msgs, 'No devices available')
            end
            return msgs
        end

        -- ─────────────────────────────────────────────────────────────────
        -- TIMER: drain queued replies, one per minute
        -- ─────────────────────────────────────────────────────────────────
        if (triggeredItem.isTimer) then
            local q = dz.data.pendingReplies
            if (#q > 0) then
                local item = table.remove(q, 1)
                -- Queued items always use channel-based routing ("to") because
                -- the plugin consumed the origin id on the first reply.
                sendReplyTo(item.channel, item.text)
                dz.data.pendingReplies = q
            end
            return
        end

        -- ─────────────────────────────────────────────────────────────────
        -- DEVICE TRIGGER: Command In changed — parse JSON payload
        -- ─────────────────────────────────────────────────────────────────
        if (not triggeredItem.isDevice) then return end

        local raw = triggeredItem.text or triggeredItem.sValue or ''
        if (raw == '') then return end

        local parseOk, parsed = pcall(dz.utils.fromJSON, raw)
        if (not parseOk or type(parsed) ~= 'table') then
            dz.log('Could not parse Command In payload: ' .. raw, dz.LOG_DEBUG)
            return
        end

        local rid     = parsed.id
        local seq     = tonumber(parsed.seq) or 0
        local body    = trim(parsed.cmd or '')
        local sender  = parsed.sender or '?'
        local channel = parsed.channel or ''

        -- Prevent re-processing the same command (plugin uses monotonic seq)
        if (seq <= dz.data.lastHandledSeq) then
            dz.log('Duplicate seq=' .. seq .. ', ignoring.', dz.LOG_DEBUG)
            return
        end
        dz.data.lastHandledSeq = seq

        if (body == '') then return end

        -- The plugin already filters for CMD_PREFIX, but verify defensively
        if (body:sub(1, #CMD_PREFIX) ~= CMD_PREFIX) then
            dz.log('No command prefix, ignoring.', dz.LOG_DEBUG)
            return
        end

        -- Strip the prefix and route
        body = trim(body:sub(#CMD_PREFIX + 1))
        if (body == '') then return end

        dz.log('Command from ' .. sender .. ' on #' .. channel .. ': ' .. body, dz.LOG_INFO)

        -- ─────────────────────────────────────────────────────────────────
        -- Route command
        -- ─────────────────────────────────────────────────────────────────
        local cmd    = lower(body)
        local reply  = nil
        local replies = nil

        if (cmd == 'help' or cmd == '?') then
            reply = cmdHelp()
        elseif (cmd == 'status' or cmd == 'all') then
            replies = cmdStatus()
        elseif (cmd == 'climate' or cmd == 'temperature') then
            reply = cmdClimate()
        elseif (cmd == 'weather') then
            reply = cmdWeather()
        elseif (cmd == 'energy' or cmd == 'power') then
            reply = cmdEnergy()
        elseif (cmd == 'home') then
            reply = cmdHome()
        elseif (cmd == 'switches') then
            reply = cmdSwitches()
        elseif (cmd == 'temp' or cmd == 'temps') then
            reply = cmdTemp()
        elseif (cmd:sub(1, 7) == 'device ') then
            local devName = trim(body:sub(8))
            reply = cmdDevice(devName)
        else
            reply = 'Unknown command. Send "' .. CMD_PREFIX .. 'help" for options.'
        end

        -- ─────────────────────────────────────────────────────────────────
        -- Queue reply/replies
        -- ─────────────────────────────────────────────────────────────────
        if (replies ~= nil and #replies > 0) then
            -- First message uses id-based routing (consumes the plugin origin record)
            sendReplyById(rid, table.remove(replies, 1))
            -- Remaining messages use explicit channel routing
            local q = dz.data.pendingReplies
            for _, msg in ipairs(replies) do
                table.insert(q, { channel = channel, text = msg })
            end
            dz.data.pendingReplies = q
            dz.log('Queued ' .. #replies .. ' additional reply message(s)', dz.LOG_INFO)
        elseif (reply ~= nil) then
            sendReplyById(rid, reply)
        end
    end
}
