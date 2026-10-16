//! Delta command framing/units ported from jat255/solar-inverter-datalogger.

pub const FIELDS: &[(&str, u8, u8, f64)] = &[
    ("DailyEnergy", 21, 45, 1000.0),
    ("Power", 51, 4, 1000.0),
    ("ACVoltage", 51, 1, 1.0),
    ("ACFrequency", 51, 2, 100.0),
    ("ACCurrent", 51, 3, 10.0),
    ("PV1Voltage", 28, 1, 1.0),
    ("PV1Current", 28, 2, 10.0),
    ("PV2Voltage", 36, 1, 1.0),
    ("PV2Current", 36, 2, 10.0),
    ("PV3Voltage", 44, 1, 1.0),
    ("PV3Current", 44, 2, 10.0),
    ("PV4Voltage", 48, 1, 1.0),
    ("PV4Current", 48, 2, 10.0),
];

pub fn crc(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in bytes.iter().skip(1) {
        crc ^= u16::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xa001
            } else {
                crc >> 1
            };
        }
    }
    crc
}

pub fn command(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![2, 5, 1, payload.len() as u8];
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&crc(&bytes).to_le_bytes());
    bytes.push(3);
    bytes
}

#[derive(Default)]
pub struct Decoder(Vec<u8>);

impl Decoder {
    // Notifications may contain partial frames or more than one frame.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.0.extend_from_slice(bytes);
        let mut frames = Vec::new();
        while self.0.len() >= 4 {
            if self.0[0] != 2 || ![6, 21].contains(&self.0[1]) || self.0[2] != 1 {
                self.0.remove(0);
                continue;
            }
            if self.0[3] < 2 {
                self.0.remove(0);
                continue;
            }
            let size = self.0[3] as usize + 7;
            if self.0.len() < size {
                break;
            }
            let frame = &self.0[..size];
            if frame[size - 1] != 3
                || crc(&frame[..size - 3]) != u16::from_le_bytes([frame[size - 3], frame[size - 2]])
            {
                self.0.remove(0);
                continue;
            }
            frames.push(self.0.drain(..size).collect());
        }
        frames
    }
}

pub fn value(frame: &[u8]) -> Option<i64> {
    if frame.len() < 10 || frame[1] != 6 {
        return None;
    }
    let data = &frame[6..frame.len() - 3];
    if data.is_empty() || data.len() > 8 {
        return None;
    }
    let mut value = 0i64;
    for b in data {
        value = value.wrapping_shl(8) | i64::from(*b);
    }
    if data.len() == 2 {
        value = i64::from(value as i16);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_power_command_and_response() {
        assert_eq!(command(&[51, 4]), [2, 5, 1, 2, 51, 4, 121, 15, 3]);
        let frame = [2, 6, 1, 4, 51, 4, 7, 39, 154, 190, 3];
        let mut decoder = Decoder::default();
        assert!(decoder.push(&frame[..5]).is_empty());
        let decoded = decoder.push(&frame[5..]);
        assert_eq!(value(&decoded[0]), Some(1831));
        let mut corrupt = frame;
        corrupt[6] ^= 1;
        assert!(decoder.push(&corrupt).is_empty());
        assert_eq!(decoder.push(&frame).len(), 1);
    }
    #[test]
    fn zero_and_unsupported() {
        assert_eq!(value(&[2, 6, 1, 4, 28, 2, 0, 0, 0, 0, 3]), Some(0));
        assert_eq!(value(&[2, 21, 1, 2, 48, 1, 120, 63, 3]), None);
    }
}
