export type SeedRenderKind = 'bump' | 'base58' | 'ascii' | 'hex';

const HEX_PREFIX = '0x';

export const toBytes = (hexValue: string): Uint8Array => {
    const normalized = hexValue.startsWith(HEX_PREFIX) ? hexValue.slice(2) : hexValue;
    if (normalized.length === 0) {
        return new Uint8Array();
    }

    const padded = normalized.length % 2 === 0 ? normalized : `0${normalized}`;
    const bytes = new Uint8Array(padded.length / 2);

    for (let i = 0; i < padded.length; i += 2) {
        bytes[i / 2] = parseInt(padded.slice(i, i + 2), 16);
    }

    return bytes;
};

const isPrintableAscii = (bytes: Uint8Array): boolean => {
    if (bytes.length === 0) {
        return false;
    }
    return bytes.every((byte) => byte >= 32 && byte <= 126);
};

export const bytesToBigInt = (bytes: Uint8Array): bigint => {
    return bytes.reduce((acc, byte) => (acc << 8n) + BigInt(byte), 0n);
};

export const asciiFromBytes = (bytes: Uint8Array): string => {
    return Array.from(bytes)
        .map((byte) => String.fromCharCode(byte))
        .join('');
};

export const classifySeed = (bytes: Uint8Array, isBump: boolean): SeedRenderKind => {
    if (isBump) {
        return 'bump';
    }
    if (bytes.length === 32) {
        return 'base58';
    }
    if (isPrintableAscii(bytes)) {
        return 'ascii';
    }
    return 'hex';
};
