const mongoose = require('mongoose');

const dividendAnnouncementSchema = new mongoose.Schema({
  symbol: { type: String, required: true },
  amountPerShare: { type: Number, required: true },
  exDate: { type: Date, required: true },
  recordDate: { type: Date },
  payDate: { type: Date, required: true },
  status: { 
    type: String, 
    enum: ['ANNOUNCED', 'CONFIRMED', 'PAID'], 
    default: 'ANNOUNCED'
  },
  type: String,
  yield: Number,
  metadata: {
    source: { type: String },
    url: { type: String },
    notes: { type: String }
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

// Indexes for efficient queries
dividendAnnouncementSchema.index({ symbol: 1, exDate: 1 }, { unique: true });
dividendAnnouncementSchema.index({ payDate: 1 });
dividendAnnouncementSchema.index({ status: 1 });

module.exports = mongoose.model('DividendAnnouncement', dividendAnnouncementSchema);
