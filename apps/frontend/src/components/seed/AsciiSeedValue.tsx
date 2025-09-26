import TogglableSeedValue from './TogglableSeedValue';

interface AsciiSeedValueProps {
    asciiValue: string;
    rawHex: string;
}

const AsciiSeedValue = ({ asciiValue, rawHex }: AsciiSeedValueProps) => {
    return <TogglableSeedValue valueA={asciiValue} valueB={rawHex} label="ASCII" />;
};

export default AsciiSeedValue;
