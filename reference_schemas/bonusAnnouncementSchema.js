const mongoose = require('mongoose');

const bonusAnnouncementSchema = new mongoose.Schema({
  symbol: { type: String, required: true },
  bonusPercentage: { type: Number, required: true }, // e.g., 10, 50, 100
  bonusType: {
    type: String,
    enum: ['STOCK_SPLIT', 'BONUS_SHARES'],
    required: true
  },
  exDate: { type: Date, required: true }, // Eligibility determination AND settlement date
  status: {
    type: String,
    enum: ['ANNOUNCED', 'CONFIRMED'],
    default: 'ANNOUNCED'
  },
  metadata: {
    source: { type: String },
    url: { type: String },
    notes: { type: String }
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

// Indexes for efficient queries
bonusAnnouncementSchema.index({ symbol: 1, exDate: 1 });
bonusAnnouncementSchema.index({ exDate: 1 });
bonusAnnouncementSchema.index({ status: 1 });

module.exports = mongoose.model('BonusAnnouncement', bonusAnnouncementSchema);
