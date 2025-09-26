import React, { useMemo } from 'react';
import bs58 from 'bs58';
import {
    asciiFromBytes,
    bytesToBigInt,
    classifySeed,
    toBytes,
} from './seed/seed-utils';
import BumpSeedValue from './seed/BumpSeedValue';
import PubkeySeedValue from './seed/PubkeySeedValue';
import AsciiSeedValue from './seed/AsciiSeedValue';
import HexSeedValue from './seed/HexSeedValue';

interface SeedValueProps {
    rawHex: string;
    isBump: boolean;
}

const SeedValue = ({ rawHex, isBump }: SeedValueProps) => {
    const bytes = useMemo(() => toBytes(rawHex), [rawHex]);
    const kind = useMemo(() => classifySeed(bytes, isBump), [bytes, isBump]);

    const decimalValue = useMemo(() => bytesToBigInt(bytes).toString(), [bytes]);
    const base58Value = useMemo(() => (bytes.length ? bs58.encode(bytes) : ''), [bytes]);
    const asciiValue = useMemo(() => asciiFromBytes(bytes), [bytes]);
    const bumpValue = useMemo(() => (bytes.length ? bytes[bytes.length - 1] : 0), [bytes]);

    let content;
    if (kind === 'bump') {
        content = <BumpSeedValue bumpValue={bumpValue} rawHex={rawHex} />;
    } else if (kind === 'base58') {
        content = <PubkeySeedValue base58Value={base58Value} />;
    } else if (kind === 'ascii') {
        content = <AsciiSeedValue asciiValue={asciiValue} rawHex={rawHex} />;
    } else {
        content = <HexSeedValue decimalValue={decimalValue} rawHex={rawHex} />;
    }

    return (
        <div className="seed-value" data-kind={kind}>
            {content}
        </div>
    );
};

export default SeedValue;
