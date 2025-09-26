import { PdaEntry } from '../types/api';
import PubkeyDisplay from './PubkeyDisplay';
import SeedValue from './SeedValue';

interface PdaEntryCardProps {
    entry: PdaEntry;
}

const PdaEntryCard = ({ entry }: PdaEntryCardProps) => (
    <article className="surface-card">
        <div className="card-header">
            <h2>Program Derived Address</h2>
        </div>

        <div className="pda-details-grid">
            <div className="pda-detail-label">
                <span className="label">Program</span>
            </div>
            <div className="pda-detail-value">
                <PubkeyDisplay pubkey={entry.program_id} />
            </div>

            <div className="pda-detail-label">
                <span className="label">PDA</span>
            </div>
            <div className="pda-detail-value">
                <PubkeyDisplay pubkey={entry.pda} />
            </div>
        </div>

        {entry.seeds.length > 0 && <div className="card-divider" />}

        <div className="pda-seeds-grid">
            {entry.seeds.map((seed) => (
                <div className="seed-item-row" key={seed.index}>
                    <div className="seed-item-header">
                        <span className="label">Seed {seed.index + 1}</span>
                        <span className="label sub-label">{seed.length} bytes</span>
                    </div>
                    <div className="seed-item-value">
                        <SeedValue rawHex={seed.raw_hex} isBump={seed.is_bump} />
                    </div>
                </div>
            ))}
        </div>

        {!entry.seeds.length && (
            <div className="pda-details-grid">
                <div className="pda-detail-label">
                    <span className="label">Seeds</span>
                </div>
                <div className="pda-detail-value">
                    <span className="label">No seeds recorded</span>
                </div>
            </div>
        )}
    </article>
);

export default PdaEntryCard;
