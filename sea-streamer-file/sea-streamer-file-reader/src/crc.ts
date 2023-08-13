/**
 * Extracted from https://crates.io/crates/crczoo
 */
function calculateCrc16(data: Buffer, poly: number, init: number): number {
    let crc = init;
    for (let d = 0; d < data.length; d++) {
        let c = data.readUInt8(d);
        let i = 0x80;
        while (i > 0) {
            let bit = (crc & 0x8000) !== 0;
            if ((c & i) !== 0) {
                bit = !bit;
            }
            crc <<= 1;
            crc &= 0xFFFF;
            if (bit) {
                crc ^= poly;
            }
            i >>= 1;
        }
    }
    return crc;
}

const CRC16_CDMA2000_POLY: number = 0xC867;
export function crc16Cdma2000(data: Buffer): number {
    return calculateCrc16(data, CRC16_CDMA2000_POLY, 0xFFFF);
}