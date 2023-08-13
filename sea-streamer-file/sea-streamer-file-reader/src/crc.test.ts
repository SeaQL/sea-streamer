import { crc16Cdma2000 } from "./crc";

test('crc16Cdma2000', () => {
    expect(crc16Cdma2000(Buffer.from("123456789"))).toStrictEqual(0x4C06);
});