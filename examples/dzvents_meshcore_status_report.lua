--[[
    MeshCore Status Report v2 (dzVents bridge)
    ===========================================
    Sends periodic home-status updates to a MeshCore channel.
    Messages are split into themed groups and spaced one-per-minute
    to avoid LoRa TX overlap.

    Requires MeshCore plugin v1.0.3+ with a Command Bridge Channel
    configured (Mode3). The bridge exposes two devices used here:

      - MeshCore Reply  (Text, unit 6)   -- write JSON message here
      - MeshCore Send   (Switch, unit 7, Push On) -- turn On to dispatch

    MESSAGE FORMAT (JSON written to MeshCore Reply)
    ------------------------------------------------
      { "to": "#ChannelName", "text": "your message" }

    The plugin resolves the channel name to the correct index automatically.
    Channel names are visible in the MeshCore dashboard and logged on startup.

    BEHAVIOUR
    ---------
    - Every hour (minute == 0): builds themed status messages and queues them.
    - Every minute: drains one queued message (so they arrive one-per-minute).
    - On presence change: sends an immediate alert.

    CONFIGURATION
    -------------
    1. Set CHANNEL_NAME to your target channel name (or index like '0').
    2. Set MESHCORE_REPLY / MESHCORE_SEND to your bridge device names.
    3. Fill DEVICES with your actual Domoticz device names.
       Set any entry to nil to silently skip it.
]]--

-- ═══════════════════════════════════════════════════════════════════
-- CONFIGURATION – adjust these to match your setup
-- ═══════════════════════════════════════════════════════════════════
local CHANNEL_NAME  = 'General'            -- Channel name (or index like '0')
local MESHCORE_REPLY = 'MeshCore Reply'    -- JSON reply output   (Text, unit 6)
local MESHCORE_SEND  = 'MeshCore Send'    -- Dispatch trigger    (Switch, unit 7)

-- Device names – set any entry to nil to skip it
local DEVICES = {
    tempIndoor  = 'Temperature - Living Room',  -- Temp (+Humidity) sensor
    tempOutdoor = 'Temperature - Outside',      -- Temp (+Humidity) sensor
    thermostat  = 'Thermostat',                 -- Setpoint device
    power       = 'Power',                      -- P1 Smart Meter (energy)
    solar       = 'Solar Power',                -- kWh / Solar device (optional)
    gas         = 'Gas',                        -- P1 Smart Meter (gas, optional)
    presence    = 'Presence',                   -- Selector switch (home/away/etc)
}
-- ═══════════════════════════════════════════════════════════════════

return {
    active = true,

    on = {
        timer = {
            'every minute',
        },
        devices = {
            DEVICES.presence,
        },
    },

    logging = {
        level = domoticz.LOG_INFO,
        marker = 'MeshCoreReport',
    },

    data = {
        lastPresence = { initial = '' },
        pendingMsgs  = { initial = {} },   -- queued messages, one sent per minute
    },

    execute = function(dz, triggeredItem)

        -- Safe device lookup: dzVents throws when a device is not found,
        -- so we use pcall to avoid crashing the whole script.
        local function safeDev(name)
            if (name == nil or name == '') then return nil end
            local ok, d = pcall(dz.devices, name)
            if ok and d then return d end
            return nil
        end

        local replyDev = safeDev(MESHCORE_REPLY)
        local sendDev  = safeDev(MESHCORE_SEND)

        if (replyDev == nil) then
            dz.log('MeshCore Reply device not found! Expected: ' .. MESHCORE_REPLY, dz.LOG_ERROR)
            return
        end
        if (sendDev == nil) then
            dz.log('MeshCore Send device not found! Expected: ' .. MESHCORE_SEND, dz.LOG_ERROR)
            return
        end

        -- Send a message to the configured channel
        local function sendMsg(message)
            local payload = dz.utils.toJSON({ to = '#' .. CHANNEL_NAME, text = message })
            dz.log('Sending to #' .. CHANNEL_NAME .. ': ' .. message, dz.LOG_INFO)
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

        -- ═══════════════════════════════════════════════════════════
        -- PRESENCE CHANGE (immediate alert)
        -- ═══════════════════════════════════════════════════════════
        if (triggeredItem.isDevice and triggeredItem.name == DEVICES.presence) then
            local status = triggeredItem.levelName or triggeredItem.state
            if (status ~= dz.data.lastPresence) then
                dz.data.lastPresence = status
                sendMsg('Presence: ' .. status)
            end
            return
        end

        -- ═══════════════════════════════════════════════════════════
        -- TIMER: drain one queued message per minute
        -- ═══════════════════════════════════════════════════════════
        local q = dz.data.pendingMsgs

        if (#q > 0) then
            sendMsg(table.remove(q, 1))
            dz.data.pendingMsgs = q
            return
        end

        -- ═══════════════════════════════════════════════════════════
        -- HOURLY: build new report on the hour (minute == 0)
        -- ═══════════════════════════════════════════════════════════
        local minute = tonumber(os.date('%M'))
        if (minute ~= 0) then
            return
        end

        local messages = {}

        -- 1) Climate
        local climate = {}
        local indoor = safeDev(DEVICES.tempIndoor)
        if (indoor) then
            table.insert(climate, 'Indoor ' .. round(indoor.temperature) .. 'C, ' .. safeVal(indoor.humidity, 0) .. '%')
        end
        local thermo = safeDev(DEVICES.thermostat)
        if (thermo) then
            table.insert(climate, 'Thermostat ' .. safeVal(thermo.setPoint, '?') .. 'C')
        end
        if (#climate > 0) then
            table.insert(messages, 'Climate: ' .. table.concat(climate, ' | '))
        end

        -- 2) Weather
        local weather = {}
        local outdoor = safeDev(DEVICES.tempOutdoor)
        if (outdoor) then
            table.insert(weather, round(outdoor.temperature) .. 'C, ' .. safeVal(outdoor.humidity, 0) .. '%')
        end
        if (#weather > 0) then
            table.insert(messages, 'Weather: ' .. table.concat(weather, ' | '))
        end

        -- 3) Energy
        local energy = {}
        local pw = safeDev(DEVICES.power)
        if (pw) then
            local use = pw.usage or 0
            local del = pw.usageDelivered or 0
            if (del > 0) then
                table.insert(energy, 'Delivering ' .. del .. 'W')
            else
                table.insert(energy, 'Using ' .. use .. 'W')
            end
        end
        local sol = safeDev(DEVICES.solar)
        if (sol) then
            local w = sol.WhActual or 0
            if (w > 0) then
                table.insert(energy, 'Solar ' .. w .. 'W')
            end
        end
        local gas = safeDev(DEVICES.gas)
        if (gas) then
            table.insert(energy, 'Gas today ' .. gas.counterToday)
        end
        if (#energy > 0) then
            table.insert(messages, 'Energy: ' .. table.concat(energy, ' | '))
        end

        if (#messages == 0) then
            dz.log('No devices available for status report', dz.LOG_DEBUG)
            return
        end

        -- Send first message now, queue the rest (one per minute)
        sendMsg(table.remove(messages, 1))
        dz.data.pendingMsgs = messages

        dz.log('Built ' .. (#messages + 1) .. ' status message(s) for #' .. CHANNEL_NAME, dz.LOG_INFO)
    end
}
